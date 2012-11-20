package com.dp.nebula.wormhole.plugins.reader.mongoreader;

import java.net.UnknownHostException;

import org.apache.log4j.Logger;

import com.dp.nebula.wormhole.common.interfaces.IParam;
import com.dp.nebula.wormhole.common.interfaces.IReaderPeriphery;
import com.dp.nebula.wormhole.common.interfaces.ISourceCounter;
import com.dp.nebula.wormhole.common.interfaces.ITargetCounter;
import com.mongodb.DB;
import com.mongodb.Mongo;
import com.mongodb.MongoURI;

/**
 * @author yukang.chen
 *
 */
public class MongoReaderPeriphery implements IReaderPeriphery {
	private final static Logger log = Logger.getLogger(MongoReaderPeriphery.class);
	
	private String inputUri = "";
	private String inputFields = "";
	private String inputQuery = "";
	private String inputSort = "";
	private String inputLimit = "";

	@Override
	public void doPost(IParam param, ITargetCounter counter) {
		// TODO Auto-generated method stub
	}

	@Override
	public void prepare(IParam param, ISourceCounter counter) {
		inputUri = param.getValue(ParamKey.inputUri, this.inputUri).trim();
		inputFields = param.getValue(ParamKey.inputFields, this.inputFields).trim();
		inputQuery = param.getValue(ParamKey.inputQuery, this.inputQuery).trim();
		inputSort = param.getValue(ParamKey.inputSort, this.inputSort).trim();
		inputLimit = param.getValue(ParamKey.inputLimit, this.inputLimit).trim();
		
		MongoURI uri = new MongoURI(inputUri);
		Mongo mongo = null;
		try {
			mongo = new Mongo(uri);
		}catch (UnknownHostException e) {
			throw new IllegalStateException(" Unable to connect to MongoDB at '" + uri + "'", e);
		}
		DB db = mongo.getDB(uri.getDatabase());
		
		//if there's a username and password
        if(uri.getUsername() != null && uri.getPassword() != null && !db.isAuthenticated()){
            boolean auth = db.authenticate(uri.getUsername(), uri.getPassword());
            if(auth) {
                log.info("Sucessfully authenticated with collection.");
            }
            else {
                throw new IllegalArgumentException("Unable to connect to collection. You have to check your username and password" );
            }
        }
        
        param.putValue(ParamKey.inputUri, this.inputUri);
        param.putValue(ParamKey.inputFields, this.inputFields);
        param.putValue(ParamKey.inputQuery, this.inputQuery);
        param.putValue(ParamKey.inputSort, this.inputSort);
        param.putValue(ParamKey.inputLimit, this.inputLimit);
        
	}

}
