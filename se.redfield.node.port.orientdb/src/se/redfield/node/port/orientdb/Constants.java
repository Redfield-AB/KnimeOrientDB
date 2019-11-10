package se.redfield.node.port.orientdb;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.knime.core.data.json.JSONCellFactory;


public class Constants {
	public static final JSONCellFactory JSON_CELL_FACTORY = new JSONCellFactory();
	public static final Set<String> ORIENTDB_SYSTEM_CLASS_NAMES = Collections.unmodifiableSet(new HashSet<String>(
			Arrays.asList("OSchedule", "OSequence", "OFunction", "_studio", "OUser", "OFunction", "OGeometryCollection",
					"OIdentity", "OLineString", "OMultiLineString", "OMultiPoint", "OMultiPolygon", "OPoint",
					"OPolygon", "ORectangle", "ORestricted", "ORole", "OSequence", "OShape", "OTriggered")));
	
	public static final String DATE_TIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.S";

}
