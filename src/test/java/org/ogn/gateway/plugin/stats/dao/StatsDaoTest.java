package org.ogn.gateway.plugin.stats.dao;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.junit.Before;
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

	Calendar calendar;

	@Before
	public void setUp() {
		calendar = new GregorianCalendar(TimeZone.getTimeZone("GMT"));
		calendar.set(Calendar.YEAR, 2015);
		calendar.set(Calendar.MONTH, 7);
		calendar.set(Calendar.DAY_OF_MONTH, 18);
		calendar.set(Calendar.HOUR_OF_DAY, 16);
		calendar.set(Calendar.MINUTE, 40);
		calendar.set(Calendar.SECOND, 15);
	}

	@Test
	@DirtiesContext
	public void test1() throws Exception {
		long timestamp = calendar.getTimeInMillis();
		try {
			dao.insertRangeRecord(timestamp, 58.23f, "TestRec1", "OGN123456", null, 1250);
			dao.insertRangeRecord(timestamp + 2 * 3600 * 1000 + 150, 58.23f, "TestRec1", "OGN123456", null, 1243);
			fail("two records with the same date (year:day) and receiver name must not be added");
		} catch (Exception ex) {
		}
	}

	@Test
	@DirtiesContext
	public void test2() throws Exception {
		long timestamp = calendar.getTimeInMillis();
		assertNotNull(dao);
		dao.insertRangeRecord(timestamp, 58.23f, "TestRec1", "OGN123456", null, 420);
		dao.updateRangeRecord(timestamp, 60.40f, "TestRec1", "FLR123456", "SP-1234", 432.5f);

		long date = TimeDateUtils.removeTime(timestamp);
		Map<String, Object> rec = dao.getRangeRecord(date, "TestRec1");
		assertNotNull(rec);
		assertEquals(60.40f, rec.get("range"));
		assertEquals("SP-1234", rec.get("aircraft_reg"));
		assertEquals("FLR123456", rec.get("aircraft_id"));
		assertEquals(432.5f, rec.get("alt"));
	}

	@Test
	@DirtiesContext
	public void test3() throws Exception {
		long timestamp = calendar.getTimeInMillis();
		long date = TimeDateUtils.removeTime(timestamp);
		assertEquals(-1, dao.getActiveReceiversCount(date));
		dao.insertActiveReceiversCount(date, 100);
		assertEquals(100, dao.getActiveReceiversCount(date));
		dao.updateActiveReceiversCount(date, 101);
		assertEquals(101, dao.getActiveReceiversCount(date));
	}

	@Test
	@DirtiesContext
	public void test4() throws Exception {
		long timestamp = calendar.getTimeInMillis();

		dao.insertRangeRecord(timestamp, 58.23f, "TestRec1", "OGN123456", null, 500);
		dao.insertRangeRecord(timestamp + 100, 120.54f, "TestRec2", "OGN123457", null, 520);
		dao.insertRangeRecord(timestamp + 200, 192.0f, "TestRec3", "OGN123458", null, 354);
		dao.insertRangeRecord(timestamp + 300, 13.65f, "TestRec4", "OGN123459", null, 332);
		dao.insertRangeRecord(timestamp + 400, 45.4f, "TestRec5", "OGN123460", null, 1230);

		List<Map<String, Object>> records = dao.getTopRangeRecords(4);
		assertNotNull(records);
		assertEquals(4, records.size());

		Map<String, Object> r1 = records.get(0);

		assertEquals(192.0f, r1.get("range"));
	}

	@Test
	@DirtiesContext
	public void test5() throws Exception {
		long timestamp = calendar.getTimeInMillis();

		long date = TimeDateUtils.removeTime(timestamp);
		dao.insertActiveReceiversCount(date, 100);

		calendar.set(Calendar.DAY_OF_MONTH, 19);

		dao.insertActiveReceiversCount(TimeDateUtils.removeTime(calendar.getTimeInMillis()), 102);

		calendar.set(Calendar.DAY_OF_MONTH, 20);
		dao.insertActiveReceiversCount(TimeDateUtils.removeTime(calendar.getTimeInMillis()), 98);

		calendar.set(Calendar.DAY_OF_MONTH, 21);
		dao.insertActiveReceiversCount(TimeDateUtils.removeTime(calendar.getTimeInMillis()), 99);

		List<Map<String, Object>> records = dao.getActiveReceiversCount(10);
		assertNotNull(records);
		assertEquals(4, records.size());

		Map<String, Object> r1 = records.get(0);
		assertEquals(99, r1.get("count"));
	}

	@Test
	@DirtiesContext
	public void test6() throws Exception {
		long timestamp = calendar.getTimeInMillis();

		dao.insertRangeRecord(timestamp, 58.23f, "TestRec1", "OGN123456", null, 500);
		dao.insertRangeRecord(timestamp + 100, 120.54f, "TestRec2", "OGN123457", null, 520);
		dao.insertRangeRecord(timestamp + 200, 192.0f, "TestRec3", "OGN123458", null, 354);
		dao.insertRangeRecord(timestamp + 300, 13.65f, "TestRec4", "OGN123459", null, 332);
		dao.insertRangeRecord(timestamp + 400, 45.4f, "TestRec5", "OGN123460", null, 1230);

		List<Map<String, Object>> records = dao.getTopRangeRecords(TimeDateUtils.removeTime(timestamp), 4);

		assertNotNull(records);
		assertEquals(4, records.size());
		Map<String, Object> r1 = records.get(0);
		assertEquals(192.0f, r1.get("range"));

		// go back 25h
		records = dao.getTopRangeRecords(TimeDateUtils.removeTime(timestamp - 25 * 3600 * 1000), 4);
		// there should not be any corresponding records
		assertEquals(0, records.size());
	}

	@Test
	@DirtiesContext
	public void test7() throws Exception {
		long timestamp = calendar.getTimeInMillis();

		long date = TimeDateUtils.removeTime(timestamp);

		assertEquals(-1, dao.getReceiverReceptionCount(date, "TestRec1"));
		dao.insertReceiverReceptionCount(date, "TestRec1", 0);
		dao.insertReceiverReceptionCount(date, "TestRec2", 0);

		assertEquals(0, dao.getReceiverReceptionCount(date, "TestRec1"));

		dao.updateReceiverReceptionCount(date, "TestRec1", 100);
		dao.updateReceiverReceptionCount(date, "TestRec2", 150);

		assertEquals(100, dao.getReceiverReceptionCount(date, "TestRec1"));
		assertEquals(150, dao.getReceiverReceptionCount(date, "TestRec2"));

		assertEquals(1, dao.getTopCountRecords(1).size());
		assertEquals(2, dao.getTopCountRecords(0).size());
	}

	@Test
	@DirtiesContext
	public void test8() throws Exception {

		long timestamp = calendar.getTimeInMillis();

		long date = TimeDateUtils.removeTime(timestamp);

		assertFalse(dao.isReceiverRegistered(date, "TestRec1"));

		assertEquals(Float.NaN, dao.getReceiverMaxAlt(date, "TestRec1"), 1e-10);


		dao.insertReceiverMaxAlt(timestamp, "TestRec1", "343430", null, 2500);
		dao.insertReceiverMaxAlt(timestamp + 30, "TestRec2", "544334", "A-BCD", 4500.5f);

		assertTrue(dao.isReceiverRegistered(date, "TestRec1"));

		assertEquals(2500f, dao.getReceiverMaxAlt(date, "TestRec1"), 1e-10);
		assertEquals(4500.5f, dao.getReceiverMaxAlt(date, "TestRec2"), 1e-10);

		dao.updateReceiverMaxAlt(timestamp + 100, "TestRec1", "343430", null, 2538);
		dao.updateReceiverMaxAlt(timestamp + 130, "TestRec2", "544334", "A-BCD", 4520.0f);

		assertEquals(2538f, dao.getReceiverMaxAlt(date, "TestRec1"), 1e-10);
		assertEquals(4520.0f, dao.getReceiverMaxAlt(date, "TestRec2"), 1e-10);

		List<Map<String, Object>> topAlts = dao.getTopAltRecords(date, 1);
		assertEquals(1, topAlts.size());
		assertEquals("TestRec2", topAlts.get(0).get("receiver_name"));
		assertEquals(4520f, topAlts.get(0).get("max_alt"));
		assertEquals("A-BCD", topAlts.get(0).get("max_alt_aircraft_reg"));
		assertEquals(timestamp + 130, (long) topAlts.get(0).get("max_alt_timestamp"));

	}

}
