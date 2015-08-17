package org.ogn.gateway.plugin.stats.dao.impl;

import static org.ogn.gateway.plugin.stats.TimeDateUtils.removeTime;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.ogn.gateway.plugin.stats.dao.StatsDAO;
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
	public void insertRangeRecord(long timestamp, float distance, String receiverName, String aircraftId,
			String aircraftReg, float aircraftAlt) {
		long date = removeTime(timestamp);
		final StringBuilder sql = new StringBuilder(
				"insert into OGN_MAX_RANGE(date, receiver_name, range, timestamp, aircraft_id, aircraft_reg, alt) ");
		sql.append("values(?,?,?,?,?,?,?)");

		jdbcTemplate.update(sql.toString(), date, receiverName, distance, timestamp, aircraftId, aircraftReg,
				aircraftAlt);
	}

	@Override
	public void updateRangeRecord(long timestamp, float distance, String receiverName, String aircraftId,
			String aircraftReg, float aircraftAlt) {

		final StringBuilder sql = new StringBuilder("update OGN_MAX_RANGE ");
		sql.append("set timestamp=?, range=?, aircraft_id=?, aircraft_reg=?, alt=?");
		sql.append("where date=? and receiver_name=?");

		jdbcTemplate.update(sql.toString(), timestamp, distance, aircraftId, aircraftReg, aircraftAlt,
				removeTime(timestamp), receiverName);
	}

	@Override
	public Map<String, Object> getRangeRecord(final long date, final String receiverName) {

		String sql = "select * from OGN_MAX_RANGE where date=? and receiver_name=?";

		RowMapper<Map<String, Object>> mapper = new RowMapper<Map<String, Object>>() {

			@Override
			public Map<String, Object> mapRow(ResultSet rs, int arg1) throws SQLException {

				Map<String, Object> rec = new HashMap<>();
				rec.put("timestamp", rs.getLong("timestamp"));
				rec.put("range", rs.getFloat("range"));
				rec.put("aircraft_id", rs.getString("aircraft_id"));
				rec.put("aircraft_reg", rs.getString("aircraft_reg"));
				rec.put("alt", rs.getFloat("alt"));
				return rec;
			}
		};

		Map<String, Object> result = null;

		try {
			result = jdbcTemplate.queryForObject(sql, mapper, new Object[] { date, receiverName });
		} catch (EmptyResultDataAccessException ex) {
			result = null;
		}

		return result;
	}

	@Override
	public void insertActiveReceiversCount(long date, int count) {
		final String sql = "insert into OGN_STATS(date, online_receivers) values(?,?)";
		jdbcTemplate.update(sql, date, count);
	}

	@Override
	public void updateActiveReceiversCount(long date, int count) {
		final String sql = "update OGN_STATS set online_receivers=? where date=?";
		jdbcTemplate.update(sql, count, date);
	}

	@Override
	public int getActiveReceiversCount(long date) {
		final String sql = "select online_receivers from OGN_STATS where date=?";

		Integer result = -1;
		try {
			result = jdbcTemplate.queryForObject(sql, Integer.class, date);
		} catch (EmptyResultDataAccessException ex) {
			// still null ;-)
			result = -1;
		}

		return result;
	}

	@Override
	public List<Map<String, Object>> getTopRangeRecords(int numRecords) {
		// String sql = "select * from OGN_MAX_RANGE order by range desc limit ?";

		String sql = "select date, receiver_name, max(range) as range, timestamp, aircraft_id, aircraft_reg, alt "
				+ "from OGN_MAX_RANGE_V group by receiver_name order by range desc limit ?";

		RowMapper<Map<String, Object>> mapper = new RowMapper<Map<String, Object>>() {

			@Override
			public Map<String, Object> mapRow(ResultSet rs, int arg1) throws SQLException {

				Map<String, Object> rec = new HashMap<>();
				rec.put("timestamp", rs.getLong("timestamp"));
				rec.put("receiver_name", rs.getString("receiver_name"));
				rec.put("range", rs.getFloat("range"));
				rec.put("aircraft_id", rs.getString("aircraft_id"));
				rec.put("aircraft_reg", rs.getString("aircraft_reg"));
				rec.put("alt", rs.getInt("alt"));
				return rec;
			}
		};

		return jdbcTemplate.query(sql, mapper, new Object[] { numRecords });
	}

	@Override
	public List<Map<String, Object>> getTopRangeRecords(long date, int numRecords) {

		String sql = "select * from OGN_MAX_RANGE where date=? order by range desc limit ?";

		RowMapper<Map<String, Object>> mapper = new RowMapper<Map<String, Object>>() {

			@Override
			public Map<String, Object> mapRow(ResultSet rs, int arg1) throws SQLException {
				Map<String, Object> rec = new HashMap<>();
				rec.put("timestamp", rs.getLong("timestamp"));
				rec.put("receiver_name", rs.getString("receiver_name"));
				rec.put("range", rs.getFloat("range"));
				rec.put("aircraft_id", rs.getString("aircraft_id"));
				rec.put("aircraft_reg", rs.getString("aircraft_reg"));
				rec.put("alt", rs.getInt("alt"));
				return rec;
			}
		};

		return jdbcTemplate.query(sql, mapper, new Object[] { date, numRecords });
	}

	@Override
	public List<Map<String, Object>> getActiveReceiversCount(int days) {
		String sql = "select * from OGN_STATS order by date limit ?";

		RowMapper<Map<String, Object>> mapper = new RowMapper<Map<String, Object>>() {

			@Override
			public Map<String, Object> mapRow(ResultSet rs, int arg1) throws SQLException {

				Map<String, Object> rec = new HashMap<>();
				rec.put("count", rs.getInt("online_receivers"));
				rec.put("date", rs.getLong("date"));
				return rec;
			}
		};

		return jdbcTemplate.query(sql, mapper, new Object[] { days });
	}

	@Override
	public void insertReceiverReceptionCount(long date, String receiverName, int count) {
		final String sql = "insert into OGN_RECEIVER(date, receiver_name, beacons_received) values(?,?,?)";
		jdbcTemplate.update(sql, date, receiverName, count);
	}

	@Override
	public void updateReceiverReceptionCount(long date, String receiverName, int count) {
		final String sql = "update OGN_RECEIVER set beacons_received=? where date=? and receiver_name=?";
		jdbcTemplate.update(sql, count, date, receiverName);
	}

	@Override
	public int getReceiverReceptionCount(long date, String receiverName) {
		final String sql = "select beacons_received from OGN_RECEIVER where date=? and receiver_name=?";

		Integer result = -1;
		try {
			result = jdbcTemplate.queryForObject(sql, Integer.class, date, receiverName);
		} catch (EmptyResultDataAccessException ex) {
			// still null ;-)
			result = -1;
		}

		return result;
	}

	@Override
	public List<Map<String, Object>> getTopCountRecords(int limit) {
		String sql = "select * from OGN_RECEIVER_V order by beacons_received desc limit ?";

		RowMapper<Map<String, Object>> mapper = new RowMapper<Map<String, Object>>() {

			@Override
			public Map<String, Object> mapRow(ResultSet rs, int arg1) throws SQLException {

				Map<String, Object> rec = new HashMap<>();
				rec.put("receiver_name", rs.getString("receiver_name"));
				rec.put("count", rs.getInt("beacons_received"));
				return rec;
			}
		};

		return jdbcTemplate.query(sql, mapper, new Object[] { limit });
	}

	@Override
	public List<Map<String, Object>> getTopCountRecords(long date, int limit) {
		String sql = "select * from OGN_RECEIVER where date=? order by beacons_received desc limit ?";

		RowMapper<Map<String, Object>> mapper = new RowMapper<Map<String, Object>>() {

			@Override
			public Map<String, Object> mapRow(ResultSet rs, int arg1) throws SQLException {

				Map<String, Object> rec = new HashMap<>();
				rec.put("receiver_name", rs.getString("receiver_name"));
				rec.put("count", rs.getInt("beacons_received"));
				return rec;
			}
		};

		return jdbcTemplate.query(sql, mapper, new Object[] { date, limit });
	}

}