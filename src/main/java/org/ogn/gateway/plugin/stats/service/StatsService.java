package org.ogn.gateway.plugin.stats.service;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public interface StatsService {

	void insertOrUpdateRangeRecord(long timestamp, float range, String receiverName, String aircraftId,
			String aircraftReg, float aircraftAlt);

	void insertOrUpdateActiveReceiversCount(long date, int count);
	
	void insertOrUpdateReceivedBeaconsCounters(long date, Map<String, AtomicInteger> counters);
}