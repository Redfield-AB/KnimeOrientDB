package se.redfield.node.port.orientdb.command;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import javax.json.JsonNumber;
import javax.json.JsonObject;
import javax.json.JsonString;
import javax.json.JsonValue;
import javax.json.JsonValue.ValueType;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OElement;

public class JsonUtils {
	
	@SuppressWarnings({ "rawtypes", "unchecked"})
	public static Map convertToMap(ODatabaseSession databaseSession,OProperty property, JsonObject jsonObject) {
		Map map = new HashMap();
		OClass linkedClass = property.getLinkedClass();
		for (Iterator<Entry<String, JsonValue>> it = jsonObject.entrySet().iterator(); it.hasNext();) {
			Entry<String, JsonValue> entry = it.next();
			if (linkedClass == null) {
				//embedded map with primitive values
				Object primitiveValue = getRequiredPrimitiveValue(property, entry.getValue());
				map.put(entry.getKey(), primitiveValue);
			} else {
				//embedded map with object values
				OElement objectValue = getRequiredObjectValue(databaseSession, property, entry.getValue());
				map.put(entry.getKey(), objectValue);
			}
		}
		return map;

	}
	
	
	public static Object getRequiredPrimitiveValue(OProperty property, JsonValue currentValue) {
		Object resultValue = null;
		if (currentValue.getValueType().equals(ValueType.STRING)) {
			JsonString jsonString = (JsonString) currentValue;
			StringBuilder builder = new StringBuilder(jsonString.getChars());
			resultValue = builder.toString();
		} else if (currentValue.getValueType().equals(ValueType.TRUE)) {
			resultValue = Boolean.TRUE;
		} else if (currentValue.getValueType().equals(ValueType.FALSE)) {
			resultValue = Boolean.FALSE;
		} else if (currentValue.getValueType().equals(ValueType.NUMBER)) {
			JsonNumber jsonNumber = (JsonNumber) currentValue;
			if (property.getLinkedType().equals(OType.INTEGER)) {
				resultValue = Integer.valueOf(jsonNumber.intValue());
			} else if (property.getLinkedType().equals(OType.LONG)) {
				resultValue = Long.valueOf(jsonNumber.longValue());
			} else if (property.getLinkedType().equals(OType.FLOAT)) {
				resultValue = Float.valueOf((float) jsonNumber.doubleValue());
			} else if (property.getLinkedType().equals(OType.DOUBLE)) {
				resultValue = Double.valueOf(jsonNumber.doubleValue());
			} else if (property.getLinkedType().equals(OType.DECIMAL)) {
				resultValue = new BigDecimal(jsonNumber.doubleValue());
			}
		}
    	return resultValue;    	
    }
	
	public static OElement getRequiredObjectValue(ODatabaseSession databaseSession,OProperty property, JsonValue currentValue) {
		OElement resultValue = null;
		JsonObject jsonObject = (JsonObject) currentValue;
		OClass linkedClass = property.getLinkedClass();
    	if (linkedClass.isEdgeType()) {
    		throw new UnsupportedOperationException("For create edge use query!");    		
    	} else if (linkedClass.isVertexType()) {
    		resultValue = databaseSession.newVertex(linkedClass);    		
    	} else {
    		resultValue = databaseSession.newInstance(linkedClass.getName());
    	}
    	for (Iterator<Entry<String,JsonValue>> it = jsonObject.entrySet().iterator();it.hasNext();) {
    		Entry<String,JsonValue> entry = it.next();
    		Object primitiveValue = getRequiredPrimitiveValue(linkedClass.getProperty(entry.getKey()),entry.getValue());
    		resultValue.setProperty(entry.getKey(), primitiveValue);
    	}	
		return resultValue;  
	}
}
