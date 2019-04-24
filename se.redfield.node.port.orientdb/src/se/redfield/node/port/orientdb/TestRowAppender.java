package se.redfield.node.port.orientdb;

import java.util.LinkedList;
import java.util.List;

import org.knime.core.data.DataRow;
import org.knime.core.data.container.RowAppender;

public class TestRowAppender implements RowAppender {
	private List<DataRow> rows = new LinkedList<DataRow>();

	@Override
	public void addRowToTable(DataRow row) {
		rows.add(row);
	}

	public List<DataRow> getRows() {
		return rows;
	}
	

}
