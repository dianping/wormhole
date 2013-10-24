package com.dp.nebula.wormhole.plugins.writer.mongowriter;

import java.net.UnknownHostException;

import org.apache.log4j.Logger;

import com.dp.nebula.wormhole.common.interfaces.IParam;
import com.dp.nebula.wormhole.common.interfaces.ISourceCounter;
import com.dp.nebula.wormhole.common.interfaces.ITargetCounter;
import com.dp.nebula.wormhole.common.interfaces.IWriterPeriphery;
import com.dp.nebula.wormhole.plugins.common.MongoUtils;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import com.mongodb.MongoURI;

public class MongoWriterPeriphery implements IWriterPeriphery {
	private static final Logger log = Logger
			.getLogger(MongoWriterPeriphery.class);
	private static final int MAX_SPLIT_NUM = 10;

	private String outputUri = "";
	private String outputFields = "";
	private int concurrency = 1;
	private int bulkInsertLine = 100000;
	private Boolean dropCollectionBeforeInsertionSwitch = false;

	@Override
	public void rollback(IParam param) {
		// TODO Auto-generated method stub
	}

	@Override
	public void doPost(IParam param, ITargetCounter counter, int faildSize) {
		// TODO Auto-generated method stub
	}

	@Override
	public void prepare(IParam param, ISourceCounter counter) {
		outputUri = param.getValue(ParamKey.outputUri, this.outputUri).trim();
		outputFields = param.getValue(ParamKey.outputFields, this.outputFields)
				.trim();
		concurrency = Math.min(
				param.getIntValue(ParamKey.concurrency, this.concurrency),
				MAX_SPLIT_NUM);
		bulkInsertLine = param.getIntValue(ParamKey.bulkInsertLine,
				this.bulkInsertLine);
		dropCollectionBeforeInsertionSwitch = param.getBooleanValue(ParamKey.dropCollectionBeforeInsertionSwitch, false);

		MongoURI uri = new MongoURI(outputUri);
		Mongo mongo = null;
		try {
			mongo = new Mongo(uri);
		} catch (UnknownHostException e) {
			throw new IllegalStateException(
					" Unable to connect to MongoDB at '" + uri + "'", e);
		}
		DB db = mongo.getDB(uri.getDatabase());

		// if there's a username and password
		if (uri.getUsername() != null && uri.getPassword() != null
				&& !db.isAuthenticated()) {
			boolean auth = db
					.authenticate(uri.getUsername(), uri.getPassword());
			if (auth) {
				log.info("Sucessfully authenticated with collection.");
			} else {
				throw new IllegalArgumentException(
						"Unable to connect to collection. You have to check your username and password");
			}
		}
		
		if (dropCollectionBeforeInsertionSwitch){
			log.info("start to drop collection " + uri.getCollection());
			DBCollection coll = MongoUtils.getCollection(uri);
			coll.drop();
			log.info("drop collection " + uri.getCollection() + " before insert data successfully");
		}

		param.putValue(ParamKey.outputUri, this.outputUri);
		param.putValue(ParamKey.outputFields, this.outputFields);
		param.putValue(ParamKey.concurrency, String.valueOf(this.concurrency));
		param.putValue(ParamKey.bulkInsertLine,
				String.valueOf(this.bulkInsertLine));
	}
}
