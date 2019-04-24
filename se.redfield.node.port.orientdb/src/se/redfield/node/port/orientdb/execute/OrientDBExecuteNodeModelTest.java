package se.redfield.node.port.orientdb.execute;

import static org.junit.Assert.*;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;

import se.redfield.node.port.orientdb.TestRowAppender;

public class OrientDBExecuteNodeModelTest {
	private static OrientDBExecuteNodeModelMock INSTANCE = new OrientDBExecuteNodeModelMock() ;
	private static ODatabasePool orientDBPool;
	
	@BeforeClass
	public static void setUp() {
		OrientDB orientDB = new OrientDB("embedded:./databases/", OrientDBConfig.defaultConfig());
		orientDB.create("test", ODatabaseType.MEMORY);
		orientDBPool = new ODatabasePool( orientDB, "test", "admin", "admin" );	
	}
	
	@AfterClass
	public static void shutdown() {
		orientDBPool.close();
	}

	@Test
	public void executeBatchScript() {
		TestRowAppender rowAppender = new TestRowAppender();
		String batchScript = "return 3";
		try (ODatabaseSession databaseSession = orientDBPool.acquire()) {
			INSTANCE.executeBatchScript(databaseSession, batchScript, rowAppender, 0);
		}
		assertFalse("No rows!",rowAppender.getRows().isEmpty());
	}
	
	private static class OrientDBExecuteNodeModelMock extends OrientDBExecuteNodeModel {

		
		
		
	}

}
