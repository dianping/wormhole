package com.dp.nebula.wormhole.plugins.writer.mongowriter;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.dp.nebula.wormhole.common.AbstractPlugin;
import com.dp.nebula.wormhole.common.interfaces.ILine;
import com.dp.nebula.wormhole.common.interfaces.ILineReceiver;
import com.dp.nebula.wormhole.common.interfaces.IWriter;
import com.dp.nebula.wormhole.plugins.common.MongoUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoException;
import com.mongodb.MongoURI;
import com.mongodb.WriteConcern;
import com.mongodb.util.JSON;

public class MongoWriter extends AbstractPlugin implements IWriter {
	private static final Logger log = Logger.getLogger(MongoWriter.class);
	private static final int MONGO_INSERT_RETRY_TIMES = 1;
	
	private String outputUri = "";
	private String outputFields = "";
	private int bulkInsertLine = 3000;
	
	private String[] fieldNames;
	private DBCollection coll;

	@Override
	public void commit() {
	}
	
	@Override
	public void init() {
		outputUri = getParam().getValue(ParamKey.outputUri, this.outputUri).trim();
		outputFields = getParam().getValue(ParamKey.outputFields, this.outputFields).trim();
		bulkInsertLine = getParam().getIntValue(ParamKey.bulkInsertLine, this.bulkInsertLine);
		
		BasicDBObject json = (BasicDBObject) JSON.parse(outputFields);
		fieldNames = json.keySet().toArray(new String[json.keySet().size()]);		
	}

	@Override
	public void connection() {
		MongoURI uri = new MongoURI(outputUri);
		log.info("try to connect " + uri.toString());
		coll = MongoUtils.getCollection(uri);
	}

	@Override
	public void finish() {
	}

	@Override
	public void write(ILineReceiver lineReceiver) {
		List<DBObject> objList = new ArrayList<DBObject>();
		ILine line = null;
		while ((line = lineReceiver.receive()) != null){
			int fieldNum = line.getFieldNum();
			DBObject obj = new BasicDBObject();
			for (int i = 0; i < fieldNum; i++) {
				obj.put(fieldNames[i], line.getField(i));
			}
			objList.add(obj);
			if (bulkInsertLine == objList.size()){
				if (bulkInsertToMongo(coll, objList)){
					getMonitor().increaseSuccessLine(objList.size());
					log.debug(objList.size() + " lines have been inserted into mongodb.");
				}
				objList.clear();
			}
		}
		if (bulkInsertLine != objList.size()){
			if (bulkInsertToMongo(coll, objList)){
				getMonitor().increaseSuccessLine(objList.size());
				log.debug(objList.size() + " lines have been inserted into mongodb.");
			}
		}
	}
	
	private boolean bulkInsertToMongo(DBCollection coll, List<DBObject> dbObjectList){
		int retryTimes = 0;
		boolean success = false;
		do {
			try {
				coll.insert(dbObjectList, WriteConcern.SAFE);
				success = true;
			} catch (MongoException mge){
				log.warn("insert mongodb failed, retryTimes:" + retryTimes, mge);
			}
		} while (!success && ++retryTimes <= MONGO_INSERT_RETRY_TIMES);
		if (!success) {
			throw new RuntimeException(
					String.format("Miss %s log since insert mongo failed with max retry limit.", 
							dbObjectList.size()));
		}
		return true;
	}
}
