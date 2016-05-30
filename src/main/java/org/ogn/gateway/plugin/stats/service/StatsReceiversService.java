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
public interface StatsReceiversService {

	// ##########################################################################
	// ## MAX RANGE
	// ##########################################################################

	void insertOrUpdateMaxRange(long timestamp, float range, String receiverName, String aircraftId, String aircraftReg,
			float aircraftAlt);

	Map<String, Object> getMaxRange(long removeTime, String string);

	List<Map<String, Object>> getTopMaxRanges(int limit);
	
	List<Map<String, Object>> getTopMaxRanges(long date, int limit);
	
	// ##########################################################################
	// ## ACTIVE RECEIVERS
	// ##########################################################################

	void insertOrUpdateActiveReceiversCounter(long date, int count);

	int getActiveReceiversCounter(long date);

	List<Map<String, Object>> getActiveReceiversCounters(int days);

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

	double getMaxAlt(long date, String receiverName);
	
	List<Map<String, Object>> getMaxAlts(long date, int limit);
}