package org.ogn.gateway.plugin.stats.service.impl;

import static org.ogn.gateway.plugin.stats.TimeDateUtils.removeTime;

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

	private StatsDAO dao;

	@Autowired
	public void setDao(StatsDAO dao) {
		this.dao = dao;
	}

	@Override
	@Transactional
	public void insertOrUpdateRangeRecord(long timestamp, float range, String receiverName, String aircraftId,
			String aircraftReg, float aircraftAlt) {

		long date = removeTime(timestamp);

		// 1. check if thre's already a record for a given receiver
		if (null == dao.getRangeRecord(date, receiverName)) {
			dao.insertRangeRecord(timestamp, range, receiverName, aircraftId, aircraftReg, aircraftAlt);
		} else {
			Map<String, Object> rec = dao.getRangeRecord(date, receiverName);
			float r = (float) rec.get("range");
			if (r < range)
				dao.updateRangeRecord(timestamp, range, receiverName, aircraftId, aircraftReg, aircraftAlt);
		}
	}

	@Override
	@Transactional
	public void insertOrUpdateActiveReceiversCount(long date, int count) {
		int dbcount = dao.getActiveReceiversCount(date);

		// check if thre's already a record for a given date
		if (-1 == dbcount) {
			dao.insertActiveReceiversCount(date, count);
		} else if (dbcount < count) {
			dao.updateActiveReceiversCount(date, count);
		}
	}

	@Override
	@Transactional
	public void insertOrUpdateReceivedBeaconsCounters(long date, Map<String, AtomicInteger> counters) {
		for (Entry<String, AtomicInteger> e : counters.entrySet()) {
			int dbcount = dao.getReceiverReceptionCount(date, e.getKey());
			if (-1 == dbcount) {
				dao.insertReceiverReceptionCount(date, e.getKey(), e.getValue().get());
			} else if (dbcount < e.getValue().get()) {// update only if new value is > (prevents faulty updates when
														// restarting the gateway)
				dao.updateReceiverReceptionCount(date, e.getKey(), e.getValue().get());
			}
		}

	}
}