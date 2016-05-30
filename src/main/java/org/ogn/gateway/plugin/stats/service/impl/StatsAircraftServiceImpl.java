package org.ogn.gateway.plugin.stats.service.impl;

import org.ogn.gateway.plugin.stats.dao.StatsDAO;
import org.ogn.gateway.plugin.stats.service.StatsAircraftService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
// will handle some aircraft-related stats. nothing yet here..
public class StatsAircraftServiceImpl implements StatsAircraftService {

	protected StatsDAO dao;

	@Autowired
	public void setDao(StatsDAO dao) {
		this.dao = dao;
	}
}