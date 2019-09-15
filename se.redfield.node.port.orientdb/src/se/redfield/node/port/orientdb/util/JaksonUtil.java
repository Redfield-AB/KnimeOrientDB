package se.redfield.node.port.orientdb.util;

import com.fasterxml.jackson.core.JsonParser.Feature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JaksonUtil {
	public static final ObjectMapper OBJECT_MAPPER = initMapper();
	
	private static ObjectMapper initMapper() {
		ObjectMapper mapper = new ObjectMapper();
		mapper.enable(Feature.ALLOW_NON_NUMERIC_NUMBERS);
		mapper.enable(Feature.ALLOW_MISSING_VALUES);
		mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
		return mapper;
	}

}
