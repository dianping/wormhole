package com.dp.nebula.wormhole.plugins.reader.mongoreader;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.log4j.Logger;

import com.dp.nebula.wormhole.common.AbstractSplitter;
import com.dp.nebula.wormhole.common.interfaces.IParam;
import com.dp.nebula.wormhole.plugins.reader.mongoreader.ParamKey;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoURI;
import com.mongodb.util.JSON;

public class MongoReaderSplitter extends AbstractSplitter {
	private final static Logger log = Logger.getLogger(MongoReaderSplitter.class);
	
	private String inputUri = "";
	private String inputFields = "";
	private String inputQuery = "";
	private String inputSort = "";
	private String inputLimit = "";
	private Boolean needSplit = false;
	private String splitKeyPattern = "{ \"_id\":1 }";
	private int splitSize = 8;
	
	@Override
	public void init(IParam jobParams){
		super.init(jobParams);
		inputUri = param.getValue(ParamKey.inputUri, this.inputUri).trim();
		inputFields = param.getValue(ParamKey.inputFields, this.inputFields).trim();
		inputQuery = param.getValue(ParamKey.inputQuery, this.inputQuery).trim();
		inputSort = param.getValue(ParamKey.inputSort, this.inputSort).trim();
		inputLimit = param.getValue(ParamKey.inputLimit, this.inputLimit).trim();
		needSplit = param.getBooleanValue(ParamKey.needSplit, this.needSplit);
		splitKeyPattern = param.getValue(ParamKey.splitKeyPattern, this.splitKeyPattern);
		splitSize = param.getIntValue(ParamKey.splitSize, this.splitSize);
	}
	
	@Override
	public List<IParam> split(){
		List<IParam> splits = new ArrayList<IParam>();
		if (!needSplit){
			splits.add(param);
			return splits;
		}else if (!StringUtils.isBlank(inputLimit)){
			int inputLimitNum = NumberUtils.toInt(this.inputLimit, 0);
			if (inputLimitNum <= 0){
				throw new IllegalArgumentException("illegal paramkey inputLimit:" + inputLimit );
			}else{
				log.info("skip() and limit() is not currently supported due to input split issues.");
				splits.add(param);
				return splits;
			}
		}
		
		MongoURI uri = new MongoURI(inputUri);
		Mongo mongo = null;
		try {
			mongo = uri.connect();
		} catch (UnknownHostException e) {
			throw new IllegalStateException( " Unable to connect to MongoDB at '" + uri + "'", e);
		}
        DB db = mongo.getDB(uri.getDatabase());
        DBCollection coll = db.getCollection(uri.getCollection());
        final CommandResult stats = coll.getStats();
        
        final boolean isSharded = stats.getBoolean( "sharded", false );
        log.info("Collection Sharded? " + isSharded);
        
        final DBObject splitKey = getInputSplitKey(splitKeyPattern);
        final String ns = coll.getFullName();
        final DBObject q = getDBObject(inputQuery);
        log.info( "Calculating unsharded input splits on namespace '" + ns 
        		+ "' with Split Key '" + splitKey.toString() 
        		+ "' and a split size of '" + splitSize + "'mb per" );
        
        /*
         * examples:
         * { splitVector : "blog.post" , keyPattern:{x:1} , min:{x:10} , max:{x:20}, maxChunkSize:200 }
         * maxChunkSize unit in MBs
         * May optionally specify 'maxSplitPoints' and 'maxChunkObjects' to avoid traversing the whole chunk
         * 
         * { splitVector : "blog.post" , keyPattern:{x:1} , min:{x:10} , max:{x:20}, force: true }
         * 'force' will produce one split point even if data is small; defaults to false
         * NOTE: This command may take a while to run
         */
        final DBObject cmd = BasicDBObjectBuilder.start("splitVector", ns).
        		add( "keyPattern", splitKey ).
        		add( "force", false ). 
        		add( "maxChunkSize", splitSize ).get();

        log.trace( "Issuing Command: " + cmd );
        CommandResult data = coll.getDB().command(cmd);

        if (data.containsField("$err")){
        	throw new IllegalArgumentException("Error calculating splits: " + data);
        }else if ((Double) data.get("ok") != 1.0){
        	throw new IllegalArgumentException("Unable to calculate input splits: " + ((String) data.get( "errmsg" )));
        }
        BasicDBList splitData = (BasicDBList)data.get("splitKeys");
        if (splitData.size() <= 1) {
        	// no splits really. Just do the whole thing data is likely small
            splits.add(createParamSplit(this.param, q, null, null)); 
        }else {
            log.info( "Calculated " + splitData.size() + " splits." );
            
            DBObject lastKey = (DBObject) splitData.get(0);
            // first "min" split
            splits.add(createParamSplit(param, q, null, lastKey));

            for (int i = 1; i < splitData.size(); i++ ){
                final DBObject tKey = (DBObject) splitData.get(i);
                splits.add(createParamSplit(param, q, lastKey, tKey) );
                lastKey = tKey;
            }
            // last "max" split
            splits.add(createParamSplit(param, q, lastKey, null)); 
        }
		
		log.info( "Calculated " + splits.size() + " split objects." );
		return splits;
	}
	
	private IParam createParamSplit(IParam param, DBObject q, DBObject min, DBObject max) {
		IParam p = param.clone();
		BasicDBObjectBuilder b = BasicDBObjectBuilder.start("$query", q);
		
		// The min() value is included in the range and the max() value is excluded.
		if (null != min) {
			b.append("$min", min);
		}
		if (null != max) {
			b.append("$max", max);
		}
		
		final DBObject query = b.get();
        log.debug( "Assembled Query: " + query );
        
        p.putValue(ParamKey.inputQuery, JSON.serialize(query));
		return p;
	}
	
    private DBObject getInputSplitKey(String splitKeyPattern) {
        try {
            final String json = splitKeyPattern;
            final DBObject obj = (DBObject) JSON.parse( json );
            return obj == null ? new BasicDBObject("_id", 1) : obj;
        }
        catch ( final Exception e ) {
            throw new IllegalArgumentException("Provided JSON String is not representable/parseable as a DBObject.", e);
        }
    }
    
    private DBObject getDBObject(String str){
        try {
            final String json = str;
            final DBObject obj = (DBObject) JSON.parse( json );
            return obj == null ? new BasicDBObject() : obj;
        }
        catch (final Exception e) {
            throw new IllegalArgumentException("Provided JSON String is not representable/parseable as a DBObject.", e);
        }
    }
}
