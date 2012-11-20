package com.dp.nebula.wormhole.plugins.common;

import org.apache.log4j.Logger;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoURI;
import com.mongodb.util.JSON;

public final class MongoUtils {
	private final static Logger log = Logger.getLogger(MongoUtils.class);
	
	private MongoUtils(){ 
		throw new AssertionError();
	}
	
	private static final Mongo.Holder mongos = new Mongo.Holder();
	
    public static DBCollection getCollection( MongoURI uri ){
        try {
            Mongo mongo = mongos.connect( uri );
            DB myDb = mongo.getDB(uri.getDatabase());
            //if there's a username and password
            if(uri.getUsername() != null && uri.getPassword() != null && !myDb.isAuthenticated()) {
                boolean auth = myDb.authenticate(uri.getUsername(), uri.getPassword());
                if(auth) {
                    log.info("Sucessfully authenticated with collection.");
                }
                else {
                    throw new IllegalArgumentException( "Unable to connect to collection." );
                }
            }
            return uri.connectCollection(mongo);
        }
        catch ( final Exception e ) {
            throw new IllegalArgumentException( "Unable to connect to collection." + e.getMessage(), e );
        }
    }
    
    public static DBObject convertStringToDBObject(String str){
    	DBObject bson = null;
    	Object obj = JSON.parse(str);
    	if (obj instanceof DBObject){
    		bson = (DBObject) obj;
    	}
    	return bson;
    }
}
