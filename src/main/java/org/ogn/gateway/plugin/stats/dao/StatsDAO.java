package org.ogn.gateway.plugin.stats.dao;

import java.util.List;
import java.util.Map;

public interface StatsDAO {

	void insertRangeRecord(long timestamp, float distance, String receiverName, String aircraftId, String aircraftReg,
			float aircraftAlt);

	void updateRangeRecord(long timestamp, float distance, String receiverName, String aircraftId, String aircraftReg,
			float aircraftAlt);

	Map<String, Object> getRangeRecord(long date, String receiverName);

	List<Map<String, Object>> getTopRangeRecords(int numRecords);

	List<Map<String, Object>> getTopRangeRecords(long date, int numRecords);

	void insertActiveReceiversCount(long date, int count);

	void updateActiveReceiversCount(long date, int count);

	int getActiveReceiversCount(long date);

	List<Map<String, Object>> getActiveReceiversCount(int days);

	void insertReceiverReceptionCount(long date, String receiverName, int count);

	void updateReceiverReceptionCount(long date, String receiverName, int count);

	int getReceiverReceptionCount(long date, String receiverName);

	List<Map<String, Object>> getTopCountRecords(int limit);

	List<Map<String, Object>> getTopCountRecords(long date, int limit);
}