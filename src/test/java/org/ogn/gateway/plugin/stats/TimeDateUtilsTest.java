package org.ogn.gateway.plugin.stats;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.text.DateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import org.junit.Test;
import org.ogn.gateway.plugin.stats.TimeDateUtils;

public class TimeDateUtilsTest {

	@Test
	public void test() {

		Calendar cal = Calendar.getInstance();
		cal.set(Calendar.YEAR, 2015);
		cal.set(Calendar.MONTH, 5);
		cal.set(Calendar.DAY_OF_MONTH, 27);
		cal.set(Calendar.HOUR_OF_DAY, 23);
		cal.set(Calendar.MINUTE, 13);
		cal.set(Calendar.SECOND, 15);
		cal.set(Calendar.MILLISECOND, 45);
		long t1 = cal.getTimeInMillis();

		long t2 = TimeDateUtils.removeTime(t1);

		assertTrue(t2 < t1);

		cal.set(Calendar.SECOND, 59);
		cal.set(Calendar.MILLISECOND, 40);
		long t3 = cal.getTimeInMillis();

		long t4 = TimeDateUtils.removeTime(t3);

		assertEquals(t2, t4);
	}
	
	@Test
	public void test2() {
		long date = TimeDateUtils.fromString("2015-08-06");
		
		DateFormat df = DateFormat.getDateTimeInstance();		
		df.setTimeZone(TimeZone.getTimeZone("UTC"));
		
		System.out.println(df.format(new Date(date)));
		
		long date2 = TimeDateUtils.removeTime(date);
		
		assertEquals(date2,date);		
	}
	
}