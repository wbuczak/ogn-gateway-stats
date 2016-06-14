package org.ogn.gateway.plugin.stats;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.text.DateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.TimeZone;

import org.junit.Test;

public class TimeDateUtilsTest {

	@Test
	public void test() {

		LocalDate ldate = LocalDate.of(2015, 5, 27);
		LocalDateTime datetime = LocalDateTime.of(ldate, LocalTime.of(23, 13, 15, 45_000_000));

		long t1 = datetime.toInstant(ZoneOffset.UTC).toEpochMilli();

		long t2 = TimeDateUtils.removeTime(t1);

		assertTrue(t2 < t1);

		LocalDateTime datetime2 = LocalDateTime.of(ldate, LocalTime.of(23, 13, 59, 40_000_000));

		long t3 = datetime2.toInstant(ZoneOffset.UTC).toEpochMilli();

		long t4 = TimeDateUtils.removeTime(t3);

		assertEquals(t2, t4);
	}

	@Test
	public void test2() {
		long date = TimeDateUtils.fromString("2015-08-06");

		DateFormat df = DateFormat.getDateTimeInstance();
		df.setTimeZone(TimeZone.getTimeZone("UTC"));

		long date2 = TimeDateUtils.removeTime(date);

		assertEquals(date2, date);
	}

}