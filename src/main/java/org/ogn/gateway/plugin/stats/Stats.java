/**
t * Copyright (c) 2015 OGN, All Rights Reserved.
 */

package org.ogn.gateway.plugin.stats;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.ogn.commons.beacon.AircraftBeacon;
import org.ogn.commons.beacon.AircraftDescriptor;
import org.ogn.commons.beacon.ReceiverBeacon;
import org.ogn.commons.beacon.forwarder.OgnAircraftBeaconForwarder;
import org.ogn.commons.beacon.forwarder.OgnReceiverBeaconForwarder;
import org.ogn.commons.utils.AprsUtils;
import org.ogn.gateway.plugin.stats.service.StatsAircraftService;
import org.ogn.gateway.plugin.stats.service.StatsReceiversService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedResource;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

/**
 * OGN gateway statistics collector plugin
 * 
 * @author wbuczak
 */
public class Stats implements OgnAircraftBeaconForwarder, OgnReceiverBeaconForwarder {

	private static Logger LOG = LoggerFactory.getLogger(Stats.class);

	private static final String VERSION = "0.0.1";

	private static final float MIN_RANGE = 50.0f; // discard everything below that

	private static final float MAX_RANGE = 300.0f; // discard everything above that

	private static final float MAX_ALT = 15000; // discard everything above that

	private static ConcurrentMap<String, ReceiverBeacon> activeReceiversCache = new ConcurrentHashMap<>();
	private static ConcurrentMap<String, AtomicInteger> dailyRecCounters = new ConcurrentHashMap<>();
	private static ConcurrentMap<String, Object[]> dailyAltCache = new ConcurrentHashMap<>();
	private static ConcurrentMap<String, Boolean> dailyUniqeAircratIds = new ConcurrentHashMap<>();

	private ClassPathXmlApplicationContext ctx;

	private static StatsAircraftService aService;
	private static StatsReceiversService rService;

	private static Object syncMonitor = new Integer(1);
	private static volatile boolean initialized = false;

	private static Future<?> receiversCountFuture;
	private static ScheduledExecutorService scheduler;

	private final ReentrantReadWriteLock fLock = new ReentrantReadWriteLock();
	private final Lock fReadLock = fLock.readLock();
	private final Lock fWriteLock = fLock.writeLock();

	@Service
	@ManagedResource(objectName = "org.ogn.gateway.plugin.stats:name=Stats", description = "OGN gateway statistics collector plugin")
	public static class StatsMBean {

		@ManagedAttribute
		public long getReceiversCacheSize() {
			return activeReceiversCache.size();
		}

		@ManagedAttribute
		public Map<String, AtomicInteger> getDailyRecCounters() {
			return dailyRecCounters;
		}

		@ManagedAttribute
		public long getAlitudesCacheSize() {
			return dailyAltCache.size();
		}

		@ManagedAttribute
		public Map<String, Object[]> getDailyAltCache() {
			return dailyAltCache;
		}

		// clean the daily caches (1 sec after midnight)
		@Scheduled(cron = "1 0 0 * * ?")
		public void cleanDailyCaches() {
			LOG.info("cleaning daily receiver reception counters");
			dailyRecCounters.clear();
			LOG.info("cleaning daily altitudes cache");
			dailyAltCache.clear();
			LOG.info("cleaning daily unique aircraft id cache");
			dailyUniqeAircratIds.clear();
		}
	}

	@Override
	public String getName() {
		return "OGN stats collector plugin";
	}

	@Override
	public String getVersion() {
		return VERSION;
	}

	@Override
	public String getDescription() {
		return "collects and stores statistics based on receiver and aircraft beacons";
	}

	@Override
	public void init() {

		synchronized (syncMonitor) {
			if (!initialized) {
				try {
					ctx = new ClassPathXmlApplicationContext("classpath:stats-application-context.xml");
					ctx.getEnvironment().setDefaultProfiles("PRO");
					rService = ctx.getBean(StatsReceiversService.class);
					aService = ctx.getBean(StatsAircraftService.class);
					initialized = true;
				} catch (Exception ex) {
					LOG.error("context initialization failed", ex);
				}

				scheduler = Executors.newScheduledThreadPool(1);

				Runnable job = () -> {
					try {
						long date = TimeDateUtils.removeTime(System.currentTimeMillis());

						LOG.debug("current activeReceiversCache size: {}", activeReceiversCache.size());
						rService.insertOrUpdateActiveReceiversCounter(date, activeReceiversCache.size());
						LOG.debug("current daily receiver counter's cache size: {}", dailyRecCounters.size());
						rService.insertOrUpdateReceptionCounters(date, dailyRecCounters);
						LOG.debug("current daily receiver max-alt cache size: {}", dailyAltCache.size());

						for (Entry<String, Object[]> entry : dailyAltCache.entrySet()) {
							fReadLock.lock();
							Object[] maxAltAircraft = entry.getValue();
							rService.insertOrUpdateMaxAlt((long) maxAltAircraft[3], entry.getKey(),
									(String) maxAltAircraft[0], (String) maxAltAircraft[1], (float) maxAltAircraft[2]);
							fReadLock.unlock();
						}

						// clear activeReceivers cache
						activeReceiversCache.clear();

						aService.insertOrUpdateUniqueAircraftReceivedCounter(date, dailyUniqeAircratIds.size());

					} catch (Exception ex) {
						LOG.error("exception caught", ex);
					}
				};

				receiversCountFuture = scheduler.scheduleAtFixedRate(job, 6, 12, TimeUnit.MINUTES); // postpone first
																									// execution 6 min
			}

		} // sync
	}

	@Override
	public void stop() {
		receiversCountFuture.cancel(true);

		if (ctx != null)
			ctx.close();
	}

	@Override
	public void onBeacon(AircraftBeacon beacon, AircraftDescriptor descriptor) {

		// remember that this aircraft has been received
		dailyUniqeAircratIds.putIfAbsent(beacon.getId(), true);

		// counters..
		if (!dailyRecCounters.containsKey(beacon.getReceiverName()))
			dailyRecCounters.put(beacon.getReceiverName(), new AtomicInteger(1));
		else
			dailyRecCounters.get(beacon.getReceiverName()).incrementAndGet();

		if (beacon.getAlt() < MAX_ALT)
			if (!dailyAltCache.containsKey(beacon.getReceiverName())) {
				Object[] maxAltAircraft = new Object[4];
				maxAltAircraft[0] = beacon.getId();
				maxAltAircraft[1] = descriptor.getRegNumber();
				maxAltAircraft[2] = beacon.getAlt();
				maxAltAircraft[3] = beacon.getTimestamp();
				fWriteLock.lock();
				dailyAltCache.put(beacon.getReceiverName(), maxAltAircraft);
				fWriteLock.unlock();
			} else {
				Object[] maxAltAircraft = dailyAltCache.get(beacon.getReceiverName());
				if ((float) maxAltAircraft[2] < beacon.getAlt()) {
					fWriteLock.lock();
					maxAltAircraft[2] = beacon.getAlt();
					fWriteLock.unlock();
				}
			}

		// if the receiver is already in the cache..
		if (activeReceiversCache.containsKey(beacon.getReceiverName())) {

			// calculate the distance between the beacon and its receiver
			float range = (float) AprsUtils.calcDistanceInKm(beacon,
					activeReceiversCache.get(beacon.getReceiverName()));

			// TODO: this should not be done on-beacon(!) - risk of too much I/O with the backend db
			// Change an update in the periodic job only (once per 5-6min is enough...)
			if (range >= MIN_RANGE && range < MAX_RANGE) {
				rService.insertOrUpdateMaxRange(beacon.getTimestamp(), range, beacon.getReceiverName(), beacon.getId(),
						descriptor.getRegNumber(), beacon.getAlt());
			}

		} // if

	}

	@Override
	public void onBeacon(ReceiverBeacon beacon) {
		activeReceiversCache.put(beacon.getId(), beacon);
	}
}
