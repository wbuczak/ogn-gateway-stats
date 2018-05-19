package org.ogn.gateway.plugin.stats.dao.impl;

import static org.ogn.gateway.plugin.stats.TimeDateUtils.removeTime;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.ogn.gateway.plugin.stats.dao.StatsDAO;
import org.ogn.gateway.plugin.stats.dao.StatsRecordField;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

@Repository
public class SpringStatsDAO implements StatsDAO {

	private JdbcTemplate jdbcTemplate;

	@Autowired
	public void setDataSource(DataSource dataSource) {
		this.jdbcTemplate = new JdbcTemplate(dataSource);
	}

	@Override
	public void upsertMaxRange(long timestamp, float distance, String receiverName, String aircraftId,
			String aircraftReg, float aircraftAlt) {
		final long date = removeTime(timestamp);
		final StringBuilder sql = new StringBuilder(
				"insert or replace into OGN_MAX_RANGE(date, receiver_name, range, timestamp, aircraft_id, aircraft_reg, alt) ");
		sql.append("values(?,?,?,?,?,?,?)");

		jdbcTemplate.update(sql.toString(), date, receiverName, distance, timestamp, aircraftId, aircraftReg,
				aircraftAlt);
	}

	@Override
	public Map<String, Object> getMaxRange(final long date, final String receiverName) {

		final String sql = "select * from OGN_MAX_RANGE where date=? and receiver_name=?";

		final RowMapper<Map<String, Object>> mapper = new RowMapper<Map<String, Object>>() {

			@Override
			public Map<String, Object> mapRow(ResultSet rs, int arg1) throws SQLException {

				final Map<String, Object> rec = new HashMap<>();
				rec.put(StatsRecordField.TIMESTAMP.getValue(), rs.getLong("timestamp"));
				rec.put(StatsRecordField.RANGE.getValue(), rs.getFloat("range"));
				rec.put(StatsRecordField.AIRCRAFT_ID.getValue(), rs.getString("aircraft_id"));
				rec.put(StatsRecordField.AIRCRAFT_REG.getValue(), rs.getString("aircraft_reg"));
				rec.put(StatsRecordField.ALT.getValue(), rs.getFloat("alt"));
				return rec;
			}
		};

		Map<String, Object> result = null;

		try {
			result = jdbcTemplate.queryForObject(sql, mapper, new Object[]{date, receiverName});
		} catch (final EmptyResultDataAccessException ex) {
			result = null;
		}

		return result;
	}

	@Override
	public int getActiveReceiversCounter(long date) {
		final String sql = "select online_receivers from OGN_DAILY_STATS where date=?";

		Integer result = -1;
		try {
			result = jdbcTemplate.queryForObject(sql, Integer.class, date);
		} catch (final EmptyResultDataAccessException ex) {
			// still null ;-)
			result = -1;
		}

		return result;
	}

	@Override
	public List<Map<String, Object>> getTopMaxRanges(int limit) {

		Object[] args = null;
		final StringBuilder sql = new StringBuilder(
				"select date, receiver_name, max(range) as range, timestamp, aircraft_id, aircraft_reg, alt "
						+ "from OGN_MAX_RANGE_V group by receiver_name order by range desc");

		if (limit == 0) {
			args = new Object[0];
		} else {
			sql.append(" limit ?");
			args = new Object[]{limit};
		}

		final RowMapper<Map<String, Object>> mapper = new RowMapper<Map<String, Object>>() {

			@Override
			public Map<String, Object> mapRow(ResultSet rs, int arg1) throws SQLException {

				final Map<String, Object> rec = new HashMap<>();
				rec.put(StatsRecordField.TIMESTAMP.getValue(), rs.getLong("timestamp"));
				rec.put(StatsRecordField.RECEIVER_NAME.getValue(), rs.getString("receiver_name"));
				rec.put(StatsRecordField.RANGE.getValue(), rs.getFloat("range"));
				rec.put(StatsRecordField.AIRCRAFT_ID.getValue(), rs.getString("aircraft_id"));
				rec.put(StatsRecordField.AIRCRAFT_REG.getValue(), rs.getString("aircraft_reg"));
				rec.put(StatsRecordField.ALT.getValue(), rs.getInt("alt"));
				return rec;
			}
		};

		return jdbcTemplate.query(sql.toString(), mapper, args);
	}

	@Override
	public List<Map<String, Object>> getTopMaxRanges(long date, int limit) {

		Object[] args = null;
		final StringBuilder sql = new StringBuilder("select * from OGN_MAX_RANGE where date=? order by range desc");

		if (limit == 0) {
			args = new Object[]{date};
		} else {
			sql.append(" limit ?");
			args = new Object[]{date, limit};
		}

		final RowMapper<Map<String, Object>> mapper = new RowMapper<Map<String, Object>>() {

			@Override
			public Map<String, Object> mapRow(ResultSet rs, int arg1) throws SQLException {
				final Map<String, Object> rec = new HashMap<>();
				rec.put(StatsRecordField.TIMESTAMP.getValue(), rs.getLong("timestamp"));
				rec.put(StatsRecordField.RECEIVER_NAME.getValue(), rs.getString("receiver_name"));
				rec.put(StatsRecordField.RANGE.getValue(), rs.getFloat("range"));
				rec.put(StatsRecordField.AIRCRAFT_ID.getValue(), rs.getString("aircraft_id"));
				rec.put(StatsRecordField.AIRCRAFT_REG.getValue(), rs.getString("aircraft_reg"));
				rec.put(StatsRecordField.ALT.getValue(), rs.getInt("alt"));
				return rec;
			}
		};

		return jdbcTemplate.query(sql.toString(), mapper, args);
	}

	@Override
	public List<Map<String, Object>> getDailyStatsForDays(int days) {

		Object[] args = null;
		final StringBuilder sql = new StringBuilder("select * from OGN_DAILY_STATS order by date desc");

		if (days == 0) {
			args = new Object[0];
		} else {
			sql.append(" limit ?");
			args = new Object[]{days};
		}

		final RowMapper<Map<String, Object>> mapper = new RowMapper<Map<String, Object>>() {

			@Override
			public Map<String, Object> mapRow(ResultSet rs, int arg1) throws SQLException {

				final Map<String, Object> rec = new HashMap<>();
				rec.put(StatsRecordField.ONLINE_RECEIVERS.getValue(), rs.getInt("online_receivers"));
				rec.put(StatsRecordField.UNIQUE_AIRCRAFT_IDS.getValue(), rs.getInt("unique_aircraft_ids"));
				rec.put(StatsRecordField.DATE.getValue(), rs.getLong("date"));
				return rec;
			}
		};

		return jdbcTemplate.query(sql.toString(), mapper, args);
	}

	@Override
	public void upsertReceptionCounter(long date, String receiverName, int count) {
		final String sql = "insert or ignore into OGN_RECEIVER(date,receiver_name) values(?,?)";
		jdbcTemplate.update(sql.toString(), date, receiverName);

		final String sql2 = "update OGN_RECEIVER set beacons_received=? where date=? and receiver_name=?";
		jdbcTemplate.update(sql2, count, date, receiverName);
	}

	@Override
	public int getReceptionCounter(long date, String receiverName) {
		final String sql = "select beacons_received from OGN_RECEIVER where date=? and receiver_name=?";

		Integer result = -1;
		try {
			result = jdbcTemplate.queryForObject(sql, Integer.class, date, receiverName);
		} catch (final EmptyResultDataAccessException ex) {
			// still null ;-)
			result = -1;
		}

		return result;
	}

	@Override
	public List<Map<String, Object>> getTopReceptionCounters(int limit) {

		Object[] args = null;
		final StringBuilder sql = new StringBuilder("select * from OGN_RECEIVER_V order by beacons_received desc");
		if (limit == 0) {
			args = new Object[0];
		} else {
			sql.append(" limit ?");
			args = new Object[]{limit};
		}

		final RowMapper<Map<String, Object>> mapper = new RowMapper<Map<String, Object>>() {

			@Override
			public Map<String, Object> mapRow(ResultSet rs, int arg1) throws SQLException {

				final Map<String, Object> rec = new HashMap<>();
				rec.put(StatsRecordField.RECEIVER_NAME.getValue(), rs.getString("receiver_name"));
				rec.put(StatsRecordField.COUNT.getValue(), rs.getInt("beacons_received"));
				return rec;
			}
		};

		return jdbcTemplate.query(sql.toString(), mapper, args);
	}

	@Override
	public List<Map<String, Object>> getTopReceptionCounters(long date, int limit) {

		Object[] args = null;
		final StringBuilder sql =
				new StringBuilder("select * from OGN_RECEIVER where date=? order by beacons_received desc");

		if (limit == 0) {
			args = new Object[]{date};
		} else {
			sql.append(" limit ?");
			args = new Object[]{date, limit};
		}

		final RowMapper<Map<String, Object>> mapper = new RowMapper<Map<String, Object>>() {

			@Override
			public Map<String, Object> mapRow(ResultSet rs, int arg1) throws SQLException {

				final Map<String, Object> rec = new HashMap<>();
				rec.put(StatsRecordField.RECEIVER_NAME.getValue(), rs.getString("receiver_name"));
				rec.put(StatsRecordField.COUNT.getValue(), rs.getInt("beacons_received"));
				return rec;
			}
		};

		return jdbcTemplate.query(sql.toString(), mapper, args);
	}

	@Override
	public float getMaxAlt(long date, String receiverName) {
		final String sql = "select max_alt from OGN_RECEIVER where date=? and receiver_name=?";

		Float result = null;
		try {
			result = jdbcTemplate.queryForObject(sql, Float.class, date, receiverName);
		} catch (final EmptyResultDataAccessException ex) {
			// nothing to be done here
		}

		return result == null ? Float.NaN : result;
	}

	@Override
	public List<Map<String, Object>> getMaxAlts(long date, int limit) {
		Object[] args = null;
		final StringBuilder sql = new StringBuilder(
				"select receiver_name,max_alt, max_alt_aircraft_id, max_alt_aircraft_reg, max_alt_timestamp");
		sql.append(" from OGN_RECEIVER").append(" where date=? and max_alt is not null order by max_alt desc");

		if (limit == 0) {
			args = new Object[]{date};
		} else {
			sql.append(" limit ?");
			args = new Object[]{date, limit};
		}

		final RowMapper<Map<String, Object>> mapper = new RowMapper<Map<String, Object>>() {

			@Override
			public Map<String, Object> mapRow(ResultSet rs, int arg1) throws SQLException {

				final Map<String, Object> rec = new HashMap<>();
				rec.put(StatsRecordField.RECEIVER_NAME.getValue(), rs.getString("receiver_name"));
				rec.put(StatsRecordField.ALT.getValue(), rs.getFloat("max_alt"));
				rec.put(StatsRecordField.AIRCRAFT_ID.getValue(), rs.getString("max_alt_aircraft_id"));
				rec.put(StatsRecordField.AIRCRAFT_REG.getValue(), rs.getString("max_alt_aircraft_reg"));
				rec.put(StatsRecordField.TIMESTAMP.getValue(), rs.getLong("max_alt_timestamp"));

				return rec;

			}
		};

		return jdbcTemplate.query(sql.toString(), mapper, args);
	}

	@Override
	public void upsertMaxAlt(long timestamp, String receiverName, String aircraftId, String aircraftReg,
			float aircraftAlt) {

		final String sql = "insert or ignore into OGN_RECEIVER(date,receiver_name) values(?,?)";

		final long date = removeTime(timestamp);
		jdbcTemplate.update(sql.toString(), date, receiverName);

		final StringBuilder sql2 = new StringBuilder(
				"update OGN_RECEIVER set max_alt=?, max_alt_aircraft_id=?, max_alt_aircraft_reg=?, max_alt_timestamp=? ")
						.append("where date=? and receiver_name=?");

		jdbcTemplate.update(sql2.toString(), aircraftAlt, aircraftId, aircraftReg, timestamp, date, receiverName);
	}

	@Override
	public void upsertDailyStats(long date, int activeReceivers, int distinctArcraftIds) {
		final String sql = "insert or ignore into OGN_DAILY_STATS(date) values(?)";
		jdbcTemplate.update(sql, date);
		final String sql2 = "update OGN_DAILY_STATS set online_receivers=?, unique_aircraft_ids=? where date=?";
		jdbcTemplate.update(sql2, activeReceivers, distinctArcraftIds, date);
	}

	@Override
	public int getDistinctAircraftReceivedCounter(long date) {
		final String sql = "select unique_aircraft_ids from OGN_DAILY_STATS where date=?";

		Integer result = -1;
		try {
			result = jdbcTemplate.queryForObject(sql, Integer.class, date);
		} catch (final EmptyResultDataAccessException ex) {
			// still null ;-)
			result = -1;
		}

		return result;
	}

}