package org.ogn.gateway.plugin.stats.dao;

import java.util.List;
import java.util.Map;

public interface StatsDAO {

	// ##########################################################################
	// ## MAX RANGE
	// ##########################################################################

	void insertOrReplaceMaxRange(long timestamp, float distance, String receiverName, String aircraftId,
			String aircraftReg, float aircraftAlt);

	Map<String, Object> getMaxRange(long date, String receiverName);

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

	void insertOrReplaceReceptionCounter(long date, String receiverName, int counter);

	int getReceptionCounter(long date, String receiverName);

	List<Map<String, Object>> getTopReceptionCounters(int limit);

	List<Map<String, Object>> getTopReceptionCounters(long date, int limit);

	// ##########################################################################
	// ## MAX ALT
	// ##########################################################################

	void insertOrReplaceMaxAlt(long timestamp, String receiverName, String aircraftId, String aircraftReg,
			float aircraftAlt);

	float getMaxAlt(long date, String receiverName);

	List<Map<String, Object>> getMaxAlts(long date, int limit);
}
