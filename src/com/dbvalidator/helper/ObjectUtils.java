package com.dbvalidator.helper;

import java.sql.Timestamp;
import java.util.TimeZone;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDate;
import org.joda.time.LocalTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class ObjectUtils {
	/**
	 * Returns whether the two objects are equal or not.
	 * null equals null.
	 * null never equals any non-null.
	 */
	public static <T> boolean equals(final T o1, final T o2) {
		boolean result;
		if (o1 == null) {
			if (o2 == null) {
				result = true;
			} else {
				result = false;
			}
		} else {
			if (o2 == null) {
				result = false;
			} else {
				result = o1.equals(o2);
			}
		}
		return result;
	}
	
	public static Timestamp convertTimestamp(final Timestamp timestamp, final TimeZone sourceTZ) {
		final DateTimeZone srcTimeZone = DateTimeZone.forID(sourceTZ.getID());
		//Remove JVM Timezone here
		final LocalTime srcTime = new LocalTime(timestamp.getTime());
		final LocalDate srcDate = new LocalDate(timestamp.getTime());
		//Set timezone as DBTimezone
		final DateTime srcDateTime = srcDate.toDateTimeAtStartOfDay(srcTimeZone);
		final DateTime srcDateTime2 = srcDateTime.plusMillis(srcTime.getMillisOfDay());

		//Convert Timestamp to UTC
		final DateTime utcDateTime = srcDateTime2.toDateTime(DateTimeZone.UTC);

		//Return the UTC timestamp, formatter reqiured as Java changes the timestamp to default JVM timezone.
		//But creating a timestamp using formatter, maintains the same Time, and appends default timezone automatically.
		final DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");
		return Timestamp.valueOf(utcDateTime.toString(fmt));

		/* Using JVM 8
		final ZoneId srcTimeZone = ZoneId.of(sourceTZ.getID());
		final ZonedDateTime srcTime = timestamp.toLocalDateTime().atZone(srcTimeZone);
		final ZonedDateTime nowUtc = srcTime.withZoneSameInstant(ZoneOffset.UTC);
		return Timestamp.valueOf(nowUtc.toLocalDateTime());
		*/
	}

}
