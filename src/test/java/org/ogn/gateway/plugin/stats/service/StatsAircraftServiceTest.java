package org.ogn.gateway.plugin.stats.service;

import static org.junit.Assert.assertEquals;
import static org.ogn.gateway.plugin.stats.TimeDateUtils.removeTime;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:stats-application-context.xml" })
@ActiveProfiles("TEST")
public class StatsAircraftServiceTest {

	@Autowired
	StatsAircraftService aService;

	@Test
	@DirtiesContext
	public void testAcriveReceiversCounter1() {
		LocalDateTime datetime = LocalDateTime.of(2015, 8, 18, 16, 40, 15);

		long timestamp = datetime.toInstant(ZoneOffset.UTC).toEpochMilli();

		long date = removeTime(timestamp);
		aService.insertOrUpdateUniqueAircraftReceivedCounter(date, 1044);
		aService.insertOrUpdateUniqueAircraftReceivedCounter(date, 15_000);
		aService.insertOrUpdateUniqueAircraftReceivedCounter(date, 7_000);

		assertEquals(15_000, aService.getDistinctAircraftReceivedCounter(date));
	}

}