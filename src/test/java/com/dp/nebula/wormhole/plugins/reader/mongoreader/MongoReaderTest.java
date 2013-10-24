package com.dp.nebula.wormhole.plugins.reader.mongoreader;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.dp.nebula.wormhole.common.BufferedLineExchanger;
import com.dp.nebula.wormhole.common.DefaultParam;
import com.dp.nebula.wormhole.common.interfaces.ILine;
import com.dp.nebula.wormhole.common.interfaces.IParam;
import com.dp.nebula.wormhole.common.interfaces.IPluginMonitor;
import com.dp.nebula.wormhole.plugins.common.MongoUtils;
import com.dp.nebula.wormhole.plugins.reader.hivereader.HiveReaderTest;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.Mongo;

import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.MongodConfig;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.runtime.Network;

public class MongoReaderTest {
	private static final String DATABASE_NAME = "embedded";
	private static final int MONGODB_PORT = 12348;

	private MongodExecutable mongodExe;
	private MongodProcess mongod;
	private Mongo mongo;

	@SuppressWarnings("deprecation")
	@Before
	public void beforeEach() throws Exception {
		MongodStarter  runtime = MongodStarter .getDefaultInstance();
	    mongodExe = runtime.prepare(new MongodConfig(Version.V2_0_1, MONGODB_PORT, Network.localhostIsIPv6()));
	    mongod = mongodExe.start();
	    mongo = new Mongo("localhost", MONGODB_PORT);
	}

	@After
	public void afterEach() throws Exception {
		mongo.close();
		if (mongod != null) {
			mongod.stop();
		}
	    if (mongodExe != null){
	    	mongodExe.stop();
	    }
	}

	@Test
	public void shouldCreateNewObjectInEmbeddedMongoDb() {
	    // given
	    DB db = mongo.getDB(DATABASE_NAME);
	    DBCollection col = db.createCollection("testCollection", new BasicDBObject());
	    //col.save(new BasicDBObject("testDoc", new Date()));
	    BasicDBObject document = new BasicDBObject();
        document.put("id", 1001);
        document.put("msg", "hello world mongoDB in Java");
        col.insert(document);
	   
        DBCursor cursor = col.find(null, MongoUtils.convertStringToDBObject("{ msg:1}"));
        while (cursor.hasNext()){
        	System.out.println(cursor.next().toString());
        }
        
        
	    //assertEquals(col.getCount(), 1);
	    
		MongoReader mongoReader = new MongoReader();
		Map<String, String> params = new HashMap<String, String>();
		params.put(ParamKey.inputUri, "mongodb://localhost:" + MONGODB_PORT + "/embedded.testCollection");
		params.put(ParamKey.inputFields, "{ msg : 1 }");
		IParam iParam = new DefaultParam(params);
		mongoReader.setParam(iParam);
		BufferedLineExchanger bufLineExchanger = HiveReaderTest.getBufferedLineExchangerInstance();
		
		IPluginMonitor pluginMonitor = mock(IPluginMonitor.class);
		mongoReader.setMonitor(pluginMonitor);
		
		mongoReader.init();
		mongoReader.connection();
		mongoReader.read(bufLineExchanger);
		ILine line = bufLineExchanger.receive();
		
		System.out.println(line.getField(0));
		assertEquals("hello world mongoDB in Java", line.getField(0));
		
		mongoReader.finish();
	}

}
