package org.ogn.gateway.plugin.stats.service;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This service handles inserting/reading statistics related to receivers
 * 
 * @author wbuczak
 *
 */
public interface StatsService {

	// ##########################################################################
	// ## MAX RANGE
	// ##########################################################################

	void insertOrUpdateMaxRange(long timestamp, float range, String receiverName, String aircraftId, String aircraftReg,
			float aircraftAlt);

	Map<String, Object> getMaxRange(long removeTime, String string);

	List<Map<String, Object>> getTopMaxRanges(int limit);

	List<Map<String, Object>> getTopMaxRanges(long date, int limit);

	// ##########################################################################
	// ## DAILY STATS
	// ##########################################################################

	void insertOrReplaceDailyStats(long date, int activeReceivers, int distinctArcraftIds);

	List<Map<String, Object>> getDailyStatsForDays(int days);

	int getActiveReceiversCounter(long date);

	int getDistinctAircraftReceivedCounter(long date);

	// ##########################################################################
	// ## AIRCRAFT BEACONS RECEPTION
	// ##########################################################################

	void insertOrUpdateReceptionCounters(long date, Map<String, AtomicInteger> counters);

	int getReceptionCounter(long date, String receiverName);

	List<Map<String, Object>> getTopReceptionCounters(int limit);

	List<Map<String, Object>> getTopReceptionCounters(long date, int limit);

	// ##########################################################################
	// ## MAX ALT
	// ##########################################################################

	void insertOrUpdateMaxAlt(long timestamp, String receiverName, String aircraftId, String aircraftReg,
			float aircraftAlt);

	float getMaxAlt(long date, String receiverName);

	List<Map<String, Object>> getMaxAlts(long date, int limit);

}