package org.ogn.gateway.plugin.stats;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;

public class TimeDateUtils {

	public static long removeTime(long timestamp) {
		Instant ins = Instant.ofEpochMilli(timestamp);
		return ins.truncatedTo(ChronoUnit.DAYS).toEpochMilli();
	}

	public static long fromString(String date) {
		LocalDate ldate = LocalDate.parse(date);
		return LocalDateTime.of(ldate.getYear(), ldate.getMonth(), ldate.getDayOfMonth(), 0, 0, 0)
				.toInstant(ZoneOffset.UTC).toEpochMilli();
	}

}