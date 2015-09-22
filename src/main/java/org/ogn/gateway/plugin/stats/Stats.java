/**
t * Copyright (c) 2015 OGN, All Rights Reserved.
 */

package org.ogn.gateway.plugin.stats;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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

	private static ConcurrentMap<String, ReceiverBeacon> activeReceiversCache = new ConcurrentHashMap<>();
	private static ConcurrentMap<String, AtomicInteger> dailyRecCounters = new ConcurrentHashMap<>();
	private static ConcurrentMap<String, Float> dailyAltCache = new ConcurrentHashMap<>();

	private static StatsService service;

	private static ClassPathXmlApplicationContext ctx;

	private static Object syncMonitor = new Integer(1);
	private static volatile boolean initialized = false;

	private static Future<?> receiversCountFuture;
	private static ScheduledExecutorService scheduler;

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
		public Map<String, Float> getDailyAltCache() {
			return dailyAltCache;
		}

		// clean the daily caches (1 sec after midnight)
		@Scheduled(cron = "1 0 0 * * ?")
		public void cleanDailyCaches() {
			LOG.info("cleaning daily receiver reception counters");
			dailyRecCounters.clear();
			LOG.info("cleaning daily altitudes cache");
			dailyAltCache.clear();
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
					service = ctx.getBean(StatsService.class);

					initialized = true;
				} catch (Exception ex) {
					LOG.error("context initialization failed", ex);
				}

				scheduler = Executors.newScheduledThreadPool(1);

				receiversCountFuture = scheduler.scheduleAtFixedRate(new Runnable() {

					@Override
					public void run() {
						try {

							long date = TimeDateUtils.removeTime(System.currentTimeMillis());

							LOG.debug("current activeReceiversCache size: {}", activeReceiversCache.size());
							service.insertOrUpdateActiveReceiversCount(date, activeReceiversCache.size());
							LOG.debug("current daily receiver counter's cache size: {}", dailyRecCounters.size());
							service.insertOrUpdateReceivedBeaconsCounters(date, dailyRecCounters);
							LOG.debug("current daily receiver max-alt cache size: {}", dailyAltCache.size());
							service.insertOrUpdateReceivedBeaconsMaxAlt(date, dailyAltCache);

							// clear activeReceivers cache
							activeReceiversCache.clear();
						} catch (Exception ex) {
							LOG.error("exception caught", ex);
						}

					}
				}, 6, 12, TimeUnit.MINUTES); // postpone first execution 6 min
			}

		}// sync
	}

	@Override
	public void stop() {
		receiversCountFuture.cancel(true);
	}

	@Override
	public void onBeacon(AircraftBeacon beacon, AircraftDescriptor descriptor) {

		// counters..
		if (!dailyRecCounters.containsKey(beacon.getReceiverName()))
			dailyRecCounters.put(beacon.getReceiverName(), new AtomicInteger(1));
		else
			dailyRecCounters.get(beacon.getReceiverName()).incrementAndGet();

		if (!dailyAltCache.containsKey(beacon.getReceiverName()))
			dailyAltCache.put(beacon.getReceiverName(), beacon.getAlt());
		else if (dailyAltCache.get(beacon.getReceiverName()) < beacon.getAlt())
			dailyAltCache.put(beacon.getReceiverName(), beacon.getAlt());

		// if the receiver is already in the cache..
		if (activeReceiversCache.containsKey(beacon.getReceiverName())) {

			// calculate the distance between the beacon and its receiver
			float range = (float) AprsUtils
					.calcDistanceInKm(beacon, activeReceiversCache.get(beacon.getReceiverName()));

			if (range >= MIN_RANGE && range < MAX_RANGE) {
				service.insertOrUpdateRangeRecord(beacon.getTimestamp(), range, beacon.getReceiverName(),
						beacon.getId(), descriptor.getRegNumber(), beacon.getAlt());
			}

		}// if

	}

	@Override
	public void onBeacon(ReceiverBeacon beacon) {
		activeReceiversCache.put(beacon.getId(), beacon);
	}
}
