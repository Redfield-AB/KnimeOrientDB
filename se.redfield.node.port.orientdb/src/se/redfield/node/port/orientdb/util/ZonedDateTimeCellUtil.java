package se.redfield.node.port.orientdb.util;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Date;

import org.knime.core.data.time.zoneddatetime.ZonedDateTimeCell;
import org.knime.core.data.time.zoneddatetime.ZonedDateTimeCellFactory;

public class ZonedDateTimeCellUtil {
	
	public static ZonedDateTimeCell create(Date date) {
		ZonedDateTime zonedDateTime = toZonedDateTime(date);		
		return (ZonedDateTimeCell) ZonedDateTimeCellFactory.create(zonedDateTime.format(DateTimeFormatter.ISO_ZONED_DATE_TIME), DateTimeFormatter.ISO_ZONED_DATE_TIME);		
	}
	
	public static Date create(ZonedDateTime zonedDateTime) {
		Calendar calendar = Calendar.getInstance();
		calendar.set(Calendar.YEAR, zonedDateTime.getYear());
		calendar.set(Calendar.DAY_OF_YEAR, zonedDateTime.getDayOfYear());
		calendar.set(Calendar.HOUR_OF_DAY, zonedDateTime.getHour());
		calendar.set(Calendar.MINUTE, zonedDateTime.getMinute());
		calendar.set(Calendar.SECOND, zonedDateTime.getSecond());
		return calendar.getTime();		
	}
	
	public static ZonedDateTime toZonedDateTime(Date utilDate) {
	    if (utilDate == null) {
	      return null;
	    }
	    final ZoneId systemDefault = ZoneId.systemDefault();
	    return ZonedDateTime.ofInstant(utilDate.toInstant(), systemDefault);
	  }

}
