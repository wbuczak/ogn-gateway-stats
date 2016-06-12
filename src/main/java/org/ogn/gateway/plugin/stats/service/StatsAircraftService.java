package org.ogn.gateway.plugin.stats.service;

public interface StatsAircraftService {

	void insertOrUpdateUniqueAircraftReceivedCounter(long date, int counter);
	
	int getDistinctAircraftReceivedCounter(long date);
	
}