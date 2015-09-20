package org.ogn.gateway.plugin.stats;

import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

public class TimeDateUtils {

	public static long removeTime(long timestamp) {
		Date date = new Date(timestamp); // timestamp now
		Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC")); // get calendar instance

		cal.setTime(date); // set cal to date
		cal.set(Calendar.HOUR_OF_DAY, 0); // set hour to midnight
		cal.set(Calendar.MINUTE, 0); // set minute in hour
		cal.set(Calendar.SECOND, 0); // set second in minute
		cal.set(Calendar.MILLISECOND, 0); // set millis in second
		Date zeroedDate = cal.getTime(); // actually computes the new Date

		return zeroedDate.getTime();
	}

	public static long fromString(String date) {
		Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

		// date format: "YYYY-MM-DD";

		String[] tokens = date.split("-");
		int year = Integer.parseInt(tokens[0]);
		int month = Integer.parseInt(tokens[1]);
		int day = Integer.parseInt(tokens[2]);

		// cal.setTime(date); // set cal to date
		cal.set(Calendar.YEAR, year);
		cal.set(Calendar.MONTH, month - 1);
		cal.set(Calendar.DAY_OF_MONTH, day);

		cal.set(Calendar.HOUR_OF_DAY, 0); // set hour to midnight
		cal.set(Calendar.MINUTE, 0); // set minute in hour
		cal.set(Calendar.SECOND, 0); // set second in minute
		cal.set(Calendar.MILLISECOND, 0); // set millis in second

		return cal.getTimeInMillis();
	}

}