package org.ogn.gateway.plugin.stats.dao;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.ogn.gateway.plugin.stats.TimeDateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:stats-application-context.xml" })
@ActiveProfiles("TEST")
public class StatsDaoTest {

	@Autowired
	StatsDAO dao;

	LocalDateTime datetime = LocalDateTime.of(2015, 8, 18, 16, 40, 15);

	@Test
	@DirtiesContext
	public void testInsertOrReplaceRange1() throws Exception {
		long timestamp = datetime.toInstant(ZoneOffset.UTC).toEpochMilli();
		dao.insertOrReplaceMaxRange(timestamp, 58.23f, "TestRec1", "OGN123456", null, 1250);
		dao.insertOrReplaceMaxRange(timestamp + 2 * 3600 * 1000 + 150, 58.23f, "TestRec1", "OGN123456", null, 1643);
		int records = dao.getTopMaxRanges(TimeDateUtils.removeTime(timestamp), 10).size();
		assertEquals(1, records);
	}

	@Test
	@DirtiesContext
	public void testInsertOrReplaceRange2() throws Exception {
		long timestamp = datetime.toInstant(ZoneOffset.UTC).toEpochMilli();

		assertNotNull(dao);
		dao.insertOrReplaceMaxRange(timestamp, 58.23f, "TestRec1", "OGN123456", null, 420);
		dao.insertOrReplaceMaxRange(timestamp, 60.40f, "TestRec1", "FLR123456", "SP-1234", 432.5f);

		long date = TimeDateUtils.removeTime(timestamp);
		Map<String, Object> rec = dao.getMaxRange(date, "TestRec1");
		assertNotNull(rec);
		assertEquals(60.40f, rec.get("range"));
		assertEquals("SP-1234", rec.get("aircraft_reg"));
		assertEquals("FLR123456", rec.get("aircraft_id"));
		assertEquals(432.5f, rec.get("alt"));
	}

	@Test
	@DirtiesContext
	public void testInsertingStatsRecord1() throws Exception {
		long timestamp = datetime.toInstant(ZoneOffset.UTC).toEpochMilli();
		long date = TimeDateUtils.removeTime(timestamp);
		assertEquals(-1, dao.getActiveReceiversCounter(date));
		dao.insertOrReplaceDailyStats(date, 55, 100);
		assertEquals(55, dao.getActiveReceiversCounter(date));
		dao.insertOrReplaceDailyStats(date, 56, 101);
		assertEquals(56, dao.getActiveReceiversCounter(date));
		assertEquals(101, dao.getDistinctAircraftReceivedCounter(date));
	}

	@Test
	@DirtiesContext
	public void testInsertingStatsRecord2() throws Exception {
		long timestamp = datetime.toInstant(ZoneOffset.UTC).toEpochMilli();

		long date = TimeDateUtils.removeTime(timestamp);
		dao.insertOrReplaceDailyStats(date, 15, 100);

		LocalDateTime datetime2 = datetime.plusDays(1); // 19

		dao.insertOrReplaceDailyStats(TimeDateUtils.removeTime(datetime2.toInstant(ZoneOffset.UTC).toEpochMilli()),
				25, 102);

		datetime2 = datetime2.plusDays(1); // 20
		dao.insertOrReplaceDailyStats(TimeDateUtils.removeTime(datetime2.toInstant(ZoneOffset.UTC).toEpochMilli()),
				25, 98);

		datetime2 = datetime2.plusDays(1); // 21
		date = TimeDateUtils.removeTime(datetime2.toInstant(ZoneOffset.UTC).toEpochMilli());
		dao.insertOrReplaceDailyStats(date, 45, 99);
		dao.insertOrReplaceDailyStats(date, 48, 55);

		List<Map<String, Object>> records = dao.getDailyStatsForDays(10);
		assertNotNull(records);
		assertEquals(4, records.size());

		Map<String, Object> r1 = records.get(0);
		assertEquals(48, r1.get("online_receivers"));
		assertEquals(55, r1.get("unique_aircraft_ids"));
	}

	@Test
	@DirtiesContext
	public void testMaxRange1() throws Exception {
		long timestamp = datetime.toInstant(ZoneOffset.UTC).toEpochMilli();

		dao.insertOrReplaceMaxRange(timestamp, 58.23f, "TestRec1", "OGN123456", null, 500);
		dao.insertOrReplaceMaxRange(timestamp + 100, 120.54f, "TestRec2", "OGN123457", null, 520);
		dao.insertOrReplaceMaxRange(timestamp + 200, 192.0f, "TestRec3", "OGN123458", null, 354);
		dao.insertOrReplaceMaxRange(timestamp + 300, 13.65f, "TestRec4", "OGN123459", null, 332);
		dao.insertOrReplaceMaxRange(timestamp + 400, 45.4f, "TestRec5", "OGN123460", null, 1230);

		List<Map<String, Object>> records = dao.getTopMaxRanges(4);
		assertNotNull(records);
		assertEquals(4, records.size());

		Map<String, Object> r1 = records.get(0);

		assertEquals(192.0f, r1.get("range"));
	}

	@Test
	@DirtiesContext
	public void testMaxRange2() throws Exception {
		long timestamp = datetime.toInstant(ZoneOffset.UTC).toEpochMilli();

		dao.insertOrReplaceMaxRange(timestamp, 58.23f, "TestRec1", "OGN123456", null, 500);
		dao.insertOrReplaceMaxRange(timestamp + 100, 120.54f, "TestRec2", "OGN123457", null, 520);
		dao.insertOrReplaceMaxRange(timestamp + 200, 192.0f, "TestRec3", "OGN123458", null, 354);
		dao.insertOrReplaceMaxRange(timestamp + 300, 13.65f, "TestRec4", "OGN123459", null, 332);
		dao.insertOrReplaceMaxRange(timestamp + 400, 45.4f, "TestRec5", "OGN123460", null, 1230);

		List<Map<String, Object>> records = dao.getTopMaxRanges(TimeDateUtils.removeTime(timestamp), 4);

		assertNotNull(records);
		assertEquals(4, records.size());
		Map<String, Object> r1 = records.get(0);
		assertEquals(192.0f, r1.get("range"));

		// go back 25h
		records = dao.getTopMaxRanges(TimeDateUtils.removeTime(timestamp - 25 * 3600 * 1000), 4);
		// there should not be any corresponding records
		assertEquals(0, records.size());
	}

	@Test
	@DirtiesContext
	public void testBeaconsReceptionCounter1() throws Exception {
		long timestamp = datetime.toInstant(ZoneOffset.UTC).toEpochMilli();

		long date = TimeDateUtils.removeTime(timestamp);

		assertEquals(-1, dao.getReceptionCounter(date, "TestRec1"));
		dao.insertOrReplaceReceptionCounter(date, "TestRec1", 0);
		dao.insertOrReplaceReceptionCounter(date, "TestRec2", 0);

		assertEquals(0, dao.getReceptionCounter(date, "TestRec1"));

		dao.insertOrReplaceReceptionCounter(date, "TestRec1", 100);
		dao.insertOrReplaceReceptionCounter(date, "TestRec2", 150);

		assertEquals(100, dao.getReceptionCounter(date, "TestRec1"));
		assertEquals(150, dao.getReceptionCounter(date, "TestRec2"));

		assertEquals(1, dao.getTopReceptionCounters(1).size());
		assertEquals(2, dao.getTopReceptionCounters(0).size());
	}

	@Test
	@DirtiesContext
	public void testDailyDistinctAircraftBeaconsReceptionCounter() throws Exception {
		long timestamp = datetime.toInstant(ZoneOffset.UTC).toEpochMilli();

		long date = TimeDateUtils.removeTime(timestamp);

		assertEquals(-1, dao.getDistinctAircraftReceivedCounter(date));

		dao.insertOrReplaceDailyStats(date, 45, 20_000);
		dao.insertOrReplaceDailyStats(date, 46, 30_000);

		assertEquals(30_000, dao.getDistinctAircraftReceivedCounter(date));

		// next day
		timestamp = datetime.plusDays(1).toInstant(ZoneOffset.UTC).toEpochMilli();
		date = TimeDateUtils.removeTime(timestamp);

		assertEquals(-1, dao.getDistinctAircraftReceivedCounter(date));

		dao.insertOrReplaceDailyStats(date, 120, 100_111);
		assertEquals(100_111, dao.getDistinctAircraftReceivedCounter(date));
	}

	@Test
	@DirtiesContext
	public void testMaxAlt1() throws Exception {
		long timestamp = datetime.toInstant(ZoneOffset.UTC).toEpochMilli();

		long date = TimeDateUtils.removeTime(timestamp);

		// assertFalse(dao.isReceiverRegistered(date, "TestRec1"));

		assertEquals(Float.NaN, dao.getMaxAlt(date, "TestRec1"), 1e-10);

		dao.insertOrReplaceMaxAlt(timestamp, "TestRec1", "343430", null, 2500);
		dao.insertOrReplaceMaxAlt(timestamp + 30, "TestRec2", "544334", "A-BCD", 4500.5f);

		// assertTrue(dao.isReceiverRegistered(date, "TestRec1"));

		assertEquals(2500f, dao.getMaxAlt(date, "TestRec1"), 1e-10);
		assertEquals(4500.5f, dao.getMaxAlt(date, "TestRec2"), 1e-10);

		dao.insertOrReplaceMaxAlt(timestamp + 100, "TestRec1", "343430", null, 2538);
		dao.insertOrReplaceMaxAlt(timestamp + 130, "TestRec2", "544334", "A-BCD", 4520.0f);

		assertEquals(2538f, dao.getMaxAlt(date, "TestRec1"), 1e-10);
		assertEquals(4520.0f, dao.getMaxAlt(date, "TestRec2"), 1e-10);

		List<Map<String, Object>> topAlts = dao.getMaxAlts(date, 1);
		assertEquals(1, topAlts.size());
		assertEquals("TestRec2", topAlts.get(0).get("receiver_name"));
		assertEquals(4520f, topAlts.get(0).get("max_alt"));
		assertEquals("A-BCD", topAlts.get(0).get("max_alt_aircraft_reg"));
		assertEquals(timestamp + 130, (long) topAlts.get(0).get("max_alt_timestamp"));
	}

}