package org.ogn.gateway.plugin.stats.service.impl;

import org.ogn.gateway.plugin.stats.dao.StatsDAO;
import org.ogn.gateway.plugin.stats.service.StatsAircraftService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class StatsAircraftServiceImpl implements StatsAircraftService {

	protected StatsDAO dao;

	@Autowired
	public void setDao(StatsDAO dao) {
		this.dao = dao;
	}

	@Override
	@Transactional
	public void insertOrUpdateUniqueAircraftReceivedCounter(long date, int counter) {
		int dbcount = dao.getDistinctAircraftReceivedCounter(date);
		// update only if new value is greater than the last in the db (prevents faulty
		// updates when restarting the gateway)
		if (-1 == dbcount || dbcount < counter) {
			dao.insertOrReplaceDistinctAircraftReceivedCounter(date, counter);
		}
	}

	@Override
	@Transactional(readOnly=true)
	public int getDistinctAircraftReceivedCounter(long date) {		
		return dao.getDistinctAircraftReceivedCounter(date);
	}
}