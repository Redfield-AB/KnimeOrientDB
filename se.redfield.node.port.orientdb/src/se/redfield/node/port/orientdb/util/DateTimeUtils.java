package se.redfield.node.port.orientdb.util;

import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;

public class DateTimeUtils {
	public static Instant toInstant(Date utilDate) {
        return Instant.ofEpochMilli(utilDate.getTime());
    }

    public static Date toDate(Instant instant) {
        try {
            return new Date(instant.toEpochMilli());
        } catch (ArithmeticException var2) {
            throw new IllegalArgumentException(var2);
        }
    }
    public static Date toDate(LocalTime localTime) {
    	Calendar calendar = Calendar.getInstance();
    	calendar.set(Calendar.HOUR_OF_DAY, localTime.getHour());
    	calendar.set(Calendar.MINUTE, localTime.getMinute());
    	calendar.set(Calendar.SECOND, localTime.getSecond());
        return calendar.getTime();
   
}
    public static Date toDate(LocalDateTime localDateTime) {
        	Calendar calendar = Calendar.getInstance();
        	calendar.set(Calendar.YEAR, localDateTime.getYear());
        	calendar.set(Calendar.DAY_OF_YEAR, localDateTime.getDayOfYear());
        	calendar.set(Calendar.HOUR_OF_DAY, localDateTime.getHour());
        	calendar.set(Calendar.MINUTE, localDateTime.getMinute());
        	calendar.set(Calendar.SECOND, localDateTime.getSecond());
            return calendar.getTime();
       
    }
    public static Date toDate(ZonedDateTime zonedDateTime) {
    	Calendar calendar = Calendar.getInstance();
    	calendar.set(Calendar.YEAR, zonedDateTime.getYear());
    	calendar.set(Calendar.DAY_OF_YEAR, zonedDateTime.getDayOfYear());
    	calendar.set(Calendar.HOUR_OF_DAY, zonedDateTime.getHour());
    	calendar.set(Calendar.MINUTE, zonedDateTime.getMinute());
    	calendar.set(Calendar.SECOND, zonedDateTime.getSecond());
    	TimeZone zone = toTimeZone(zonedDateTime.getZone());
    	calendar.setTimeZone(zone);
        return calendar.getTime();
   
}
    
    public static Date toDate(LocalDate localDateTime) {
    	Calendar calendar = Calendar.getInstance();
    	calendar.set(Calendar.YEAR, localDateTime.getYear());
    	calendar.set(Calendar.DAY_OF_YEAR, localDateTime.getDayOfYear());
    	calendar.set(Calendar.HOUR_OF_DAY, 0);
    	calendar.set(Calendar.MINUTE, 0);
    	calendar.set(Calendar.SECOND, 0);
        return calendar.getTime();
   
}

    public static Instant toInstant(Calendar calendar) {
        return Instant.ofEpochMilli(calendar.getTimeInMillis());
    }

    public static ZonedDateTime toZonedDateTime(Calendar calendar) {
        Instant instant = Instant.ofEpochMilli(calendar.getTimeInMillis());
        ZoneId zone = toZoneId(calendar.getTimeZone());
        return ZonedDateTime.ofInstant(instant, zone);
    }

    public static GregorianCalendar toGregorianCalendar(ZonedDateTime zdt) {
        TimeZone zone = toTimeZone(zdt.getZone());
        GregorianCalendar cal = new GregorianCalendar(zone);
        cal.setGregorianChange(new Date(-9223372036854775808L));
        cal.setFirstDayOfWeek(2);
        cal.setMinimalDaysInFirstWeek(4);

        try {
            cal.setTimeInMillis(zdt.toInstant().toEpochMilli());
            return cal;
        } catch (ArithmeticException var4) {
            throw new IllegalArgumentException(var4);
        }
    }

    public static ZoneId toZoneId(TimeZone timeZone) {
        return ZoneId.of(timeZone.getID(), ZoneId.SHORT_IDS);
    }

    public static TimeZone toTimeZone(ZoneId zoneId) {
        String tzid = zoneId.getId();
        if (!tzid.startsWith("+") && !tzid.startsWith("-")) {
            if (tzid.equals("Z")) {
                tzid = "UTC";
            }
        } else {
            tzid = "GMT" + tzid;
        }

        return TimeZone.getTimeZone(tzid);
    }

    public static LocalDate toLocalDate(java.sql.Date sqlDate) {
        return LocalDate.of(sqlDate.getYear() + 1900, sqlDate.getMonth() + 1, sqlDate.getDate());
    }

    public static java.sql.Date toSqlDate(LocalDate date) {
        return new java.sql.Date(date.getYear() - 1900, date.getMonthValue() - 1, date.getDayOfMonth());
    }

    public static LocalTime toLocalTime(Time sqlTime) {
        return LocalTime.of(sqlTime.getHours(), sqlTime.getMinutes(), sqlTime.getSeconds());
    }

    

    public static LocalDateTime toLocalDateTime(Timestamp sqlTimestamp) {
        return LocalDateTime.of(sqlTimestamp.getYear() + 1900, sqlTimestamp.getMonth() + 1, sqlTimestamp.getDate(), sqlTimestamp.getHours(), sqlTimestamp.getMinutes(), sqlTimestamp.getSeconds(), sqlTimestamp.getNanos());
    }
    
    public static LocalDateTime toLocalDateTime(Date date) {
        return LocalDateTime.of(date.getYear() + 1900, date.getMonth() + 1, date.getDate(), date.getHours(), date.getMinutes(), date.getSeconds(), 0);
    }

    public static Timestamp toSqlTimestamp(Instant instant) {
        try {
            Timestamp ts = new Timestamp(instant.getEpochSecond() * 1000L);
            ts.setNanos(instant.getNano());
            return ts;
        } catch (ArithmeticException var2) {
            throw new IllegalArgumentException(var2);
        }
    }

    public static Instant toInstant(Timestamp sqlTimestamp) {
        return Instant.ofEpochSecond(sqlTimestamp.getTime() / 1000L, (long)sqlTimestamp.getNanos());
    }
}
