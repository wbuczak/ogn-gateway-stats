package org.ogn.gateway.plugin.stats.dao;

public enum StatsRecordField {
	TIMESTAMP("timestamp"),
	RANGE("range"),
	AIRCRAFT_ID("aircraft_id"),
	AIRCRAFT_REG("aircraft_reg"),
	ALT("alt"),
	RECEIVER_NAME("receiver_name"),
	ONLINE_RECEIVERS("online_receivers"),
	UNIQUE_AIRCRAFT_IDS("unique_aircraft_ids"),
	DATE("date"),
	COUNT("count");

	private final String value;

	private StatsRecordField(String field) {
		this.value = field;
	}

	public String getValue() {
		return value;
	}
}
