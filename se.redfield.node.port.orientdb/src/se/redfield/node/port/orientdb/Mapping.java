package se.redfield.node.port.orientdb;

import org.knime.core.data.DataType;
import org.knime.core.data.collection.ListCell;
import org.knime.core.data.date.DateAndTimeCell;
import org.knime.core.data.def.BooleanCell;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.def.LongCell;
import org.knime.core.data.def.StringCell;

import com.orientechnologies.orient.core.metadata.schema.OType;

public class Mapping {
	
	
	public static DataType mapToDataType(OType oType) {
		DataType dataType = null;
		switch (oType) {
		case STRING:
		case LINK:
			dataType = StringCell.TYPE;
			break;		
		case INTEGER:
		case SHORT:
			dataType = IntCell.TYPE;
			break;
		case LONG:
			dataType = LongCell.TYPE;
			break;
		case FLOAT:
		case DOUBLE:
			dataType = DoubleCell.TYPE;
			break;
		case BOOLEAN:
			dataType = BooleanCell.TYPE;
			break;
		case DATE:			
		case DATETIME:
			dataType = DateAndTimeCell.TYPE;
			break;
		case LINKBAG:
		case EMBEDDEDLIST:
			dataType = ListCell.getCollectionType(StringCell.TYPE);
			break;		
			
		default:
			break;
		}
		return dataType;
	}
}
