package se.redfield.node.port.orientdb;

import org.knime.core.data.json.JSONCellFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

public class Constants {
	public static final JSONCellFactory JSON_CELL_FACTORY = new JSONCellFactory();
	public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
}
