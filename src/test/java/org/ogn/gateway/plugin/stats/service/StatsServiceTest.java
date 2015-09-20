package org.ogn.gateway.plugin.stats.service;

import static org.junit.Assert.assertEquals;
import static org.ogn.gateway.plugin.stats.TimeDateUtils.removeTime;

import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.ogn.gateway.plugin.stats.dao.StatsDAO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:stats-application-context.xml" })
@ActiveProfiles("TEST")
public class StatsServiceTest {

	@Autowired
	StatsService service;

	@Autowired
	StatsDAO dao;

	@Test
	@DirtiesContext
	public void test1() {

		Calendar calendar = new GregorianCalendar(TimeZone.getTimeZone("GMT"));
		calendar.set(Calendar.YEAR, 2015);
		calendar.set(Calendar.MONTH, 7);
		calendar.set(Calendar.DAY_OF_MONTH, 18);
		calendar.set(Calendar.HOUR_OF_DAY, 16);
		calendar.set(Calendar.MINUTE, 40);
		calendar.set(Calendar.SECOND, 15);

		long timestamp = calendar.getTimeInMillis();
		service.insertOrUpdateRangeRecord(timestamp, 58.23f, "TestRec1", "OGN123456", null, 600);
		service.insertOrUpdateRangeRecord(timestamp, 60.23f, "TestRec1", "OGN123456", null, 600);
		service.insertOrUpdateRangeRecord(timestamp, 57.80f, "TestRec1", "FLR543533", null, 600);

		Map<String, Object> rec = dao.getRangeRecord(removeTime(timestamp), "TestRec1");
		// the record with largest distance for a day should be remembered
		assertEquals(60.23f, rec.get("range"));

		// goto next day, just after midnight
		calendar.set(Calendar.DAY_OF_MONTH, 19);
		calendar.set(Calendar.HOUR_OF_DAY, 0);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 01);

		long timestamp2 = calendar.getTimeInMillis();
		service.insertOrUpdateRangeRecord(timestamp2, 120.0f, "TestRec1", "OGN123456", null, 720);

		rec = dao.getRangeRecord(removeTime(timestamp), "TestRec1");
		// the record with largest distance for a day should be remembered
		assertEquals(60.23f, rec.get("range"));

		rec = dao.getRangeRecord(removeTime(timestamp2), "TestRec1");
		// the record with largest distance for a day should be remembered
		assertEquals(120.0f, rec.get("range"));
	}

	@Test
	@DirtiesContext
	public void test2() {

		Calendar calendar = new GregorianCalendar(TimeZone.getTimeZone("GMT"));
		calendar.set(Calendar.YEAR, 2015);
		calendar.set(Calendar.MONTH, 7);
		calendar.set(Calendar.DAY_OF_MONTH, 18);
		calendar.set(Calendar.HOUR_OF_DAY, 16);
		calendar.set(Calendar.MINUTE, 40);
		calendar.set(Calendar.SECOND, 15);

		long timestamp = calendar.getTimeInMillis();
		long date = removeTime(timestamp);
		service.insertOrUpdateActiveReceiversCount(date, 100);
		service.insertOrUpdateActiveReceiversCount(date, 101);
		service.insertOrUpdateActiveReceiversCount(date, 102);

		assertEquals(102, dao.getActiveReceiversCount(date));
	}

	@Test
	@DirtiesContext
	public void test3() {
		Calendar calendar = new GregorianCalendar(TimeZone.getTimeZone("GMT"));
		calendar.set(Calendar.YEAR, 2015);
		calendar.set(Calendar.MONTH, 7);
		calendar.set(Calendar.DAY_OF_MONTH, 18);
		calendar.set(Calendar.HOUR_OF_DAY, 16);
		calendar.set(Calendar.MINUTE, 40);
		calendar.set(Calendar.SECOND, 15);

		long timestamp = calendar.getTimeInMillis();
		long date = removeTime(timestamp);

		Map<String, AtomicInteger> counters = new HashMap<String, AtomicInteger>();
		counters.put("Rec1", new AtomicInteger(4));
		counters.put("Rec2", new AtomicInteger(100));
		counters.put("Rec3", new AtomicInteger(120));
		counters.put("Rec4", new AtomicInteger(40));

		service.insertOrUpdateReceivedBeaconsCounters(date, counters);

		service.insertOrUpdateActiveReceiversCount(date, 100);
		service.insertOrUpdateActiveReceiversCount(date, 101);
		service.insertOrUpdateActiveReceiversCount(date, 102);

		assertEquals(120, dao.getReceiverReceptionCount(date, "Rec3"));
	}

	@Test
	@DirtiesContext
	public void test4() {
		Calendar calendar = new GregorianCalendar(TimeZone.getTimeZone("GMT"));
		calendar.set(Calendar.YEAR, 2015);
		calendar.set(Calendar.MONTH, 7);
		calendar.set(Calendar.DAY_OF_MONTH, 18);
		calendar.set(Calendar.HOUR_OF_DAY, 16);
		calendar.set(Calendar.MINUTE, 40);
		calendar.set(Calendar.SECOND, 15);

		long timestamp = calendar.getTimeInMillis();
		long date = removeTime(timestamp);

		Map<String, Float> alts = new HashMap<String, Float>();
		alts.put("Rec1", 2050.0f);
		alts.put("Rec2", 3234.5f);
		alts.put("Rec3", 560.0f);
		alts.put("Rec4", 1200f);

		service.insertOrUpdateReceivedBeaconsMaxAlt(date, alts);

		alts.put("Rec2", 3100f);

		service.insertOrUpdateReceivedBeaconsMaxAlt(date, alts);

		// still the previous (greater) alt should be returned
		assertEquals(3234.5f, dao.getReceiverMaxAlt(date, "Rec2"), 1e-10);
	}

}