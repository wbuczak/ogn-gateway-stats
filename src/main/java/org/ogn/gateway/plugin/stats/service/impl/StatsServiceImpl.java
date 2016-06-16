package org.ogn.gateway.plugin.stats.service.impl;

import static org.ogn.gateway.plugin.stats.TimeDateUtils.removeTime;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import org.ogn.gateway.plugin.stats.dao.StatsDAO;
import org.ogn.gateway.plugin.stats.service.StatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class StatsServiceImpl implements StatsService {

	protected StatsDAO dao;

	@Autowired
	public void setDao(StatsDAO dao) {
		this.dao = dao;
	}

	@Override
	@Transactional
	public void insertOrUpdateMaxRange(long timestamp, float range, String receiverName, String aircraftId,
			String aircraftReg, float aircraftAlt) {

		long date = removeTime(timestamp);

		Map<String, Object> rec = dao.getMaxRange(date, receiverName);

		// if there's no record for a given receiver or the last know max-range is smaller than the current one
		if (null == rec || (float) rec.get("range") < range) {
			dao.insertOrReplaceMaxRange(timestamp, range, receiverName, aircraftId, aircraftReg, aircraftAlt);
		}
	}

	@Override
	@Transactional
	public void insertOrUpdateReceptionCounters(long date, Map<String, AtomicInteger> counters) {
		for (Entry<String, AtomicInteger> e : counters.entrySet()) {
			int dbcount = dao.getReceptionCounter(date, e.getKey());
			// update only if new value is greater than the last in the db (prevents faulty
			// updates when restarting the gateway)
			if (-1 == dbcount || dbcount < e.getValue().get()) {
				dao.insertOrReplaceReceptionCounter(date, e.getKey(), e.getValue().get());
			}
		}
	}

	@Override
	@Transactional
	public void insertOrUpdateMaxAlt(long timestamp, String receiverName, String aircraftId, String aircraftReg,
			float aircraftAlt) {
		long date = removeTime(timestamp);
		float dbalt = dao.getMaxAlt(date, receiverName);
		if (Float.isNaN(dbalt) || dbalt < aircraftAlt)
			dao.insertOrReplaceMaxAlt(timestamp, receiverName, aircraftId, aircraftReg, aircraftAlt);
	}

	@Override
	@Transactional(readOnly = true)
	public int getActiveReceiversCounter(long date) {
		return dao.getActiveReceiversCounter(date);
	}

	@Override
	@Transactional(readOnly = true)
	public Map<String, Object> getMaxRange(long date, String receiverName) {
		return dao.getMaxRange(date, receiverName);
	}

	@Override
	@Transactional(readOnly = true)
	public float getMaxAlt(long date, String receiverName) {
		return dao.getMaxAlt(date, receiverName);
	}

	@Override
	@Transactional(readOnly = true)
	public int getReceptionCounter(long date, String receiverName) {
		return dao.getReceptionCounter(date, receiverName);
	}

	@Override
	@Transactional(readOnly = true)
	public List<Map<String, Object>> getMaxAlts(long date, int limit) {
		return dao.getMaxAlts(date, limit);
	}

	@Override
	@Transactional(readOnly = true)
	public List<Map<String, Object>> getTopReceptionCounters(int limit) {
		return dao.getTopMaxRanges(limit);
	}

	@Override
	@Transactional(readOnly = true)
	public List<Map<String, Object>> getTopReceptionCounters(long date, int limit) {
		return dao.getTopReceptionCounters(date, limit);
	}

	@Override
	@Transactional(readOnly = true)
	public List<Map<String, Object>> getTopMaxRanges(int limit) {
		return dao.getTopMaxRanges(limit);
	}

	@Override
	@Transactional(readOnly = true)
	public List<Map<String, Object>> getTopMaxRanges(long date, int limit) {
		return dao.getTopMaxRanges(date, limit);
	}

	@Override
	@Transactional
	public void insertOrReplaceDailyStats(long date, int activeReceivers, int distinctAircraftIds) {
		int dbActiveReceivers = dao.getActiveReceiversCounter(date);
		int dbAircraftIds = dao.getDistinctAircraftReceivedCounter(date);

		int updateReceivers = activeReceivers > dbActiveReceivers ? activeReceivers : dbActiveReceivers;
		int updateAircraftIds = distinctAircraftIds > dbAircraftIds ? distinctAircraftIds : dbAircraftIds;

		dao.insertOrReplaceDailyStats(date, updateReceivers, updateAircraftIds);
	}

	@Override
	@Transactional(readOnly = true)
	public List<Map<String, Object>> getDailyStatsForDays(int days) {
		return dao.getDailyStatsForDays(days);
	}

	@Override
	@Transactional(readOnly = true)
	public int getDistinctAircraftReceivedCounter(long date) {
		return dao.getDistinctAircraftReceivedCounter(date);
	}

}