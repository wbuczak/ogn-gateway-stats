package org.ogn.gateway.plugin.stats.service;

import static org.junit.Assert.assertEquals;
import static org.ogn.gateway.plugin.stats.TimeDateUtils.removeTime;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:stats-application-context.xml"})
@ActiveProfiles("TEST")
public class StatsServiceTest {

	@Autowired
	StatsService service;

	@Test
	@DirtiesContext
	public void testMaxRange1() {
		LocalDateTime datetime = LocalDateTime.of(2015, 8, 18, 16, 40, 15);

		long timestamp = datetime.toInstant(ZoneOffset.UTC).toEpochMilli();

		service.upsertMaxRange(timestamp, 58.23f, "TestRec1", "OGN123456", null, 600);
		service.upsertMaxRange(timestamp, 60.23f, "TestRec1", "OGN123456", null, 600);
		service.upsertMaxRange(timestamp, 57.80f, "TestRec1", "FLR543533", null, 600);

		Map<String, Object> rec = service.getMaxRange(removeTime(timestamp), "TestRec1");
		// the record with largest distance for a day should be remembered
		assertEquals(60.23f, rec.get("range"));

		// set next day, just after midnight
		LocalDateTime datetime2 = datetime.plusDays(1).minusHours(16).minusMinutes(40).minusSeconds(14);

		long timestamp2 = datetime2.toInstant(ZoneOffset.UTC).toEpochMilli();

		service.upsertMaxRange(timestamp2, 120.0f, "TestRec1", "OGN123456", null, 720);

		rec = service.getMaxRange(removeTime(timestamp), "TestRec1");
		// the record with largest distance for a day should be remembered
		assertEquals(60.23f, rec.get("range"));

		rec = service.getMaxRange(removeTime(timestamp2), "TestRec1");
		// the record with largest distance for a day should be remembered
		assertEquals(120.0f, rec.get("range"));
	}

	@Test
	@DirtiesContext
	public void testDailyStats() {
		LocalDateTime datetime = LocalDateTime.of(2015, 8, 18, 16, 40, 15);

		long timestamp = datetime.toInstant(ZoneOffset.UTC).toEpochMilli();

		long date = removeTime(timestamp);
		service.upsertDailyStats(date, 540, 1300);
		service.upsertDailyStats(date, 550, 1000);

		assertEquals(1300, service.getDistinctAircraftReceivedCounter(date));
		assertEquals(550, service.getActiveReceiversCounter(date));
	}

	@Test
	@DirtiesContext
	public void testReceivedBeaconsCounters() {
		LocalDateTime datetime = LocalDateTime.of(2015, 8, 18, 16, 40, 15);

		long timestamp = datetime.toInstant(ZoneOffset.UTC).toEpochMilli();

		long date = removeTime(timestamp);

		Map<String, AtomicInteger> counters = new HashMap<String, AtomicInteger>();
		counters.put("Rec1", new AtomicInteger(4));
		counters.put("Rec2", new AtomicInteger(100));
		counters.put("Rec3", new AtomicInteger(120));
		counters.put("Rec4", new AtomicInteger(40));

		service.upsertReceptionCounters(date, counters);

		// simulate restart - counter is smaller than the last in the db
		counters = new HashMap<String, AtomicInteger>();
		counters.put("Rec3", new AtomicInteger(10));
		service.upsertReceptionCounters(date, counters);

		assertEquals(120, service.getReceptionCounter(date, "Rec3"));

	}

	@Test
	@DirtiesContext
	public void testReceivedBeaxonsMaxAlt() {

		LocalDateTime datetime = LocalDateTime.of(2015, 8, 18, 16, 40, 15);

		long timestamp = datetime.toInstant(ZoneOffset.UTC).toEpochMilli();

		long date = removeTime(timestamp);

		service.upsertMaxAlt(date, "Rec1", "343433", "SP-ABC", 2050.0f);
		service.upsertMaxAlt(date, "Rec2", "443433", null, 3234.5f);
		service.upsertMaxAlt(date, "Rec3", "656543", null, 560.0f);
		service.upsertMaxAlt(date, "Rec4", "554343", null, 1200f);

		service.upsertMaxAlt(date, "Rec2", "443433", null, 3100f);

		// still the previous (greater) alt should be returned
		assertEquals(3234.5f, service.getMaxAlt(date, "Rec2"), 1e-10);
	}

}