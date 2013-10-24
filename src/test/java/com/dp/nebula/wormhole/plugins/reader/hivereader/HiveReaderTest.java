package com.dp.nebula.wormhole.plugins.reader.hivereader;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dp.nebula.wormhole.common.BufferedLineExchanger;
import com.dp.nebula.wormhole.common.DefaultParam;
import com.dp.nebula.wormhole.common.interfaces.ILine;
import com.dp.nebula.wormhole.common.interfaces.IParam;
import com.dp.nebula.wormhole.common.interfaces.IPluginMonitor;
import com.dp.nebula.wormhole.common.utils.ParseXMLUtil;
import com.dp.nebula.wormhole.engine.config.EngineConfParamKey;
import com.dp.nebula.wormhole.engine.storage.StorageConf;
import com.dp.nebula.wormhole.engine.storage.StorageManager;

public class HiveReaderTest {

	private static Testdb db = null;
	private final static String DB_NAME = "hivedb";
	
	public static BufferedLineExchanger getBufferedLineExchangerInstance(){
		IParam engineConf = null;
		engineConf = ParseXMLUtil.loadEngineConfig();
		List<StorageConf> result = new ArrayList<StorageConf>();
		
		for(int i = 0; i< 5; i++){
			StorageConf storageConf = new StorageConf();
			storageConf.setId(String.valueOf(i));
			storageConf.setStorageClassName(
					engineConf.getValue(EngineConfParamKey.STORAGE_CLASS_NAME));
			storageConf.setLineLimit(
					engineConf.getIntValue(EngineConfParamKey.STORAGE_LINE_LIMIT));
			storageConf.setByteLimit(
					engineConf.getIntValue(EngineConfParamKey.STORAGE_BYTE_LIMIT));
			storageConf.setDestructLimit(
					engineConf.getIntValue(EngineConfParamKey.STORAGE_DESTRUCT_LIMIT));
			storageConf.setPeriod(
					engineConf.getIntValue(EngineConfParamKey.MONITOR_INFO_DISPLAY_PERIOD));
			storageConf.setWaitTime(
					engineConf.getIntValue(EngineConfParamKey.STORAGE_WAIT_TIME));
			result.add(storageConf);
		}
		StorageManager manager = new StorageManager(result);
		BufferedLineExchanger exchanger = new BufferedLineExchanger(manager.getStorageForWriter("1"), manager.getStorageForReader());
		return exchanger;
	}

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		try {
			db = new Testdb(DB_NAME);
		} catch (Exception ex1) {
			ex1.printStackTrace(); // could not start db
			return; // bye bye
		}
		db.update("CREATE TABLE sample_table ( id INTEGER IDENTITY, str_col VARCHAR(256), num_col INTEGER)");

		db.update("INSERT INTO sample_table(str_col,num_col) VALUES('Ford', 100)");
//		db.update("INSERT INTO sample_table(str_col,num_col) VALUES('Toyota', 200)");
//		db.update("INSERT INTO sample_table(str_col,num_col) VALUES('Honda', 300)");
//		db.update("INSERT INTO sample_table(str_col,num_col) VALUES('GM', 400)");
		
		//db.query("SELECT * FROM sample_table WHERE num_col < 250");
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		if (db != null){
			db.shutdown();
		}
	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testHiveReader() {
		HiveReader hiveReader = new HiveReader();
		
		Map<String, String> params = new HashMap<String, String>();
		params.put(ParamKey.path, "jdbc:hsqldb:mem:" + DB_NAME);
		params.put(ParamKey.username, "sa");
		params.put(ParamKey.sql, "SELECT str_col, num_col FROM sample_table WHERE num_col < 250");
		IParam iParam = new DefaultParam(params);
		hiveReader.setParam(iParam);
		BufferedLineExchanger bufLineExchanger = HiveReaderTest.getBufferedLineExchangerInstance();
		
		IPluginMonitor pluginMonitor = mock(IPluginMonitor.class);
		hiveReader.setMonitor(pluginMonitor);
		
		hiveReader.init();
		hiveReader.connection();
		hiveReader.read(bufLineExchanger);
		ILine line = bufLineExchanger.receive();
		
		assertEquals(2, line.getFieldNum());
		assertEquals("Ford", line.getField(0));
		assertEquals("100", line.getField(1));
		
		hiveReader.finish();
	}
	
	static class Testdb {

	    Connection conn;                                                //our connnection to the db - presist for life of program

	    // we dont want this garbage collected until we are done
	    public Testdb(String dbname) throws Exception {    // note more general exception

	        // Load the HSQL Database Engine JDBC driver
	        // hsqldb.jar should be in the class path or made part of the current jar
	        Class.forName("org.hsqldb.jdbcDriver");

	        // connect to the database.   This will load the db files and start the
	        // database if it is not alread running.
	        // db_file_name_prefix is used to open or create files that hold the state
	        // of the db.
	        // It can contain directory names relative to the
	        // current working directory
	        conn = DriverManager.getConnection("jdbc:hsqldb:"
	                                           + "mem:" + dbname ,       // filenames
	                                           "sa",                     // username
	                                           "");                      // password
	    }

	    public void shutdown() throws SQLException {

	        Statement st = conn.createStatement();
	        
	        // db writes out to files and performs clean shuts down
	        // otherwise there will be an unclean shutdown
	        // when program ends
	        st.execute("SHUTDOWN");
	        conn.close();    // if there are no other open connection
	    }

	//use for SQL command SELECT
	    public synchronized void query(String expression) throws SQLException {

	        Statement st = null;
	        ResultSet rs = null;

	        st = conn.createStatement();         // statement objects can be reused with

	        // repeated calls to execute but we
	        // choose to make a new one each time
	        rs = st.executeQuery(expression);    // run the query

	        // do something with the result set.
	        dump(rs);
	        st.close();    // NOTE!! if you close a statement the associated ResultSet is

	        // closed too
	        // so you should copy the contents to some other object.
	        // the result set is invalidated also  if you recycle an Statement
	        // and try to execute some other query before the result set has been
	        // completely examined.
	    }

	//use for SQL commands CREATE, DROP, INSERT and UPDATE
	    public synchronized void update(String expression) throws SQLException {

	        Statement st = null;

	        st = conn.createStatement();    // statements

	        int i = st.executeUpdate(expression);    // run the query

	        if (i == -1) {
	            System.out.println("db error : " + expression);
	        }

	        st.close();
	    }    // void update()

	    public static void dump(ResultSet rs) throws SQLException {

	        // the order of the rows in a cursor
	        // are implementation dependent unless you use the SQL ORDER statement
	        ResultSetMetaData meta   = rs.getMetaData();
	        int               colmax = meta.getColumnCount();
	        int               i;
	        Object            o = null;

	        // the result set is a cursor into the data.  You can only
	        // point to one row at a time
	        // assume we are pointing to BEFORE the first row
	        // rs.next() points to next row and returns true
	        // or false if there is no next row, which breaks the loop
	        for (; rs.next(); ) {
	            for (i = 0; i < colmax; ++i) {
	                o = rs.getObject(i + 1);    // Is SQL the first column is indexed

	                // with 1 not 0
	                System.out.print(o.toString() + " ");
	            }

	            System.out.println(" ");
	        }
	    }                                       //void dump( ResultSet rs )

	    public static void main(String[] args) {

	        Testdb db = null;

	        try {
	            db = new Testdb("db_file");
	        } catch (Exception ex1) {
	            ex1.printStackTrace();    // could not start db

	            return;                   // bye bye
	        }

	        try {

	            //make an empty table
	            //
	            // by declaring the id column IDENTITY, the db will automatically
	            // generate unique values for new rows- useful for row keys
	            db.update(
	                "CREATE TABLE sample_table ( id INTEGER IDENTITY, str_col VARCHAR(256), num_col INTEGER)");
	        } catch (SQLException ex2) {

	            //ignore
	            //ex2.printStackTrace();  // second time we run program
	            //  should throw execption since table
	            // already there
	            //
	            // this will have no effect on the db
	        }

	        try {

	            // add some rows - will create duplicates if run more then once
	            // the id column is automatically generated
	            db.update(
	                "INSERT INTO sample_table(str_col,num_col) VALUES('Ford', 100)");
	            db.update(
	                "INSERT INTO sample_table(str_col,num_col) VALUES('Toyota', 200)");
	            db.update(
	                "INSERT INTO sample_table(str_col,num_col) VALUES('Honda', 300)");
	            db.update(
	                "INSERT INTO sample_table(str_col,num_col) VALUES('GM', 400)");

	            // do a query
	            db.query("SELECT * FROM sample_table WHERE num_col < 250");

	            // at end of program
	            db.shutdown();
	        } catch (SQLException ex3) {
	            ex3.printStackTrace();
	        }
	    }    // main()
	}    // class Testdb
}
