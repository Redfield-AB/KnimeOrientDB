package se.redfield.node.port.orientdb.execute;

import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.function.Function;

import se.redfield.node.port.orientdb.Constants;

public class StringValuePreparer implements Function<Object, String> {

	@Override
	public String apply(Object obj) {
		if (mustBeEscaped(obj)) {
			return "\"" + (obj.toString()) + "\"";
		} else if (obj instanceof Date) {
			SimpleDateFormat dateFormat = new SimpleDateFormat(Constants.DATE_TIME_FORMAT);
			return dateFormat.format((Date) obj);
		}
		return  obj.toString();
	}
	
	boolean mustBeEscaped(Object obj) {
		return (obj instanceof String) || (obj instanceof Date) || (obj instanceof URI);
	}

}
