/**
t * Copyright (c) 2015 OGN, All Rights Reserved.
 */

package org.ogn.gateway.plugin.stats;

import java.util.LinkedList;
import java.util.List;
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
import org.ogn.gateway.plugin.stats.service.StatsService;
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
	private static List<Object[]> maxRangeCache = new LinkedList<>();

	private static ConcurrentMap<String, AtomicInteger> dailyRecCounters = new ConcurrentHashMap<>();
	private static ConcurrentMap<String, Object[]> dailyAltCache = new ConcurrentHashMap<>();
	private static ConcurrentMap<String, Boolean> dailyDistinctAircratIds = new ConcurrentHashMap<>();

	private ClassPathXmlApplicationContext ctx;

	private static StatsService service;

	private static Object syncMonitor = new Integer(1);
	private static volatile boolean initialized = false;

	private static Future<?> receiversCountFuture;
	private static ScheduledExecutorService scheduler;

	private final ReentrantReadWriteLock maxAltLock = new ReentrantReadWriteLock();
	private final Lock maxAltReadLock = maxAltLock.readLock();
	private final Lock maxAltWriteLock = maxAltLock.writeLock();

	private final ReentrantReadWriteLock maxRangeLock = new ReentrantReadWriteLock();
	private final Lock maxRangeReadLock = maxRangeLock.readLock();
	private final Lock maxRangeWriteLock = maxRangeLock.writeLock();

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

		@ManagedAttribute
		public int getDailyDistinctAircratIdsCounter() {
			return dailyDistinctAircratIds.size();
		}

		@ManagedAttribute
		public int getMaxRangeCacheSize() {
			return maxRangeCache.size();
		}
	}

	// clean the daily caches (1 sec after midnight)
	@Scheduled(cron = "1 0 0 * * ?")
	public void cleanDailyCaches() {
		LOG.info("cleaning daily receiver reception counters");
		dailyRecCounters.clear();
		LOG.info("cleaning daily altitudes cache");
		dailyAltCache.clear();
		LOG.info("cleaning daily distinct aircraft id cache");
		dailyDistinctAircratIds.clear();
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
					service = ctx.getBean(StatsService.class);
					initialized = true;
				} catch (Exception ex) {
					LOG.error("context initialization failed", ex);
				}

				scheduler = Executors.newScheduledThreadPool(1);

				Runnable job = () -> {
					try {
						long date = TimeDateUtils.removeTime(System.currentTimeMillis());

						LOG.debug("current activeReceiversCache size: {}", activeReceiversCache.size());
						LOG.debug("current dailyDistinctAircratIds size: {}", dailyDistinctAircratIds.size());

						service.insertOrReplaceDailyStats(date, activeReceiversCache.size(),
								dailyDistinctAircratIds.size());

						LOG.debug("current daily receiver counter's cache size: {}", dailyRecCounters.size());
						service.insertOrUpdateReceptionCounters(date, dailyRecCounters);
						LOG.debug("current daily receiver max-alt cache size: {}", dailyAltCache.size());

						for (Entry<String, Object[]> entry : dailyAltCache.entrySet()) {
							maxAltReadLock.lock();
							Object[] maxAltAircraft = entry.getValue();
							service.insertOrUpdateMaxAlt((long) maxAltAircraft[3], entry.getKey(),
									(String) maxAltAircraft[0], (String) maxAltAircraft[1], (float) maxAltAircraft[2]);
							maxAltReadLock.unlock();
						}

						// clear activeReceivers cache
						activeReceiversCache.clear();

						maxRangeReadLock.lock();
						for (Object[] entry : maxRangeCache) {
							service.insertOrUpdateMaxRange((Long) entry[0], (Float) entry[1], (String) entry[2],
									(String) entry[3], (String) entry[4], (Float) entry[5]);
						}
						// clear max range cache
						maxRangeCache.clear();
						maxRangeReadLock.unlock();

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
		dailyDistinctAircratIds.putIfAbsent(beacon.getId(), true);

		// counters..
		if (dailyRecCounters.putIfAbsent(beacon.getReceiverName(), new AtomicInteger(1)) != null)
			dailyRecCounters.get(beacon.getReceiverName()).incrementAndGet();

		if (beacon.getAlt() < MAX_ALT)
			if (!dailyAltCache.containsKey(beacon.getReceiverName())) {
				Object[] maxAltAircraft = new Object[4];
				maxAltAircraft[0] = beacon.getId();
				maxAltAircraft[1] = descriptor.getRegNumber();
				maxAltAircraft[2] = beacon.getAlt();
				maxAltAircraft[3] = beacon.getTimestamp();
				maxAltWriteLock.lock();
				dailyAltCache.put(beacon.getReceiverName(), maxAltAircraft);
				maxAltWriteLock.unlock();
			} else {
				Object[] maxAltAircraft = dailyAltCache.get(beacon.getReceiverName());
				if ((float) maxAltAircraft[2] < beacon.getAlt()) {
					maxAltWriteLock.lock();
					maxAltAircraft[2] = beacon.getAlt();
					maxAltWriteLock.unlock();
				}
			}

		// if the receiver is already in the cache..
		if (activeReceiversCache.containsKey(beacon.getReceiverName())) {

			// calculate the distance between the beacon and its receiver
			float range = (float) AprsUtils.calcDistanceInKm(beacon,
					activeReceiversCache.get(beacon.getReceiverName()));

			if (range >= MIN_RANGE && range < MAX_RANGE) {
				maxRangeWriteLock.lock();
				maxRangeCache.add(new Object[] { beacon.getTimestamp(), range, beacon.getReceiverName(), beacon.getId(),
						descriptor.getRegNumber(), beacon.getAlt() });
				maxRangeWriteLock.unlock();
			}

		} // if

	}

	@Override
	public void onBeacon(ReceiverBeacon beacon) {
		activeReceiversCache.put(beacon.getId(), beacon);
	}
}
