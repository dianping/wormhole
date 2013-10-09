package com.dp.nebula.wormhole.plugins.reader.mongoreader;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.bson.BSONObject;

import com.dp.nebula.wormhole.common.AbstractPlugin;
import com.dp.nebula.wormhole.common.interfaces.ILine;
import com.dp.nebula.wormhole.common.interfaces.ILineSender;
import com.dp.nebula.wormhole.common.interfaces.IReader;
import com.dp.nebula.wormhole.common.interfaces.ITransformer;
import com.dp.nebula.wormhole.plugins.common.MongoUtils;
import com.dp.nebula.wormhole.transform.common.TransformerFactory;
import com.google.common.base.Joiner;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoURI;

public class MongoReader extends AbstractPlugin implements IReader {
	private static final Logger log = Logger.getLogger(MongoReader.class);
	
	private String inputUri = "";
	private String inputFields = "";
	private String inputQuery = "";
	private String inputSort = "";
	private int inputLimit = 0;
	private Boolean needSplit = false;
	private String splitKeyPattern = "{ \"_id\":1 }";
	private int splitSize = 8;
	
	private MongoURI uri = null;
	private DBObject fields = null;
	private DBObject query = null;
	private DBObject sort = null;
	private Set<String> fieldsSet;
	private boolean allFieldsOrNot = true;
	private boolean fieldNeedSplit = false;
	private char fieldSplitChar = 0;
	private String dataTransform;
	
	private transient DBCursor cursor = null;
	private BSONObject current = null;
	
	@Override
	public void init(){
		fieldsSet = new HashSet<String>();
		
		inputUri = getParam().getValue(ParamKey.inputUri, this.inputUri).trim();
		inputFields = getParam().getValue(ParamKey.inputFields, this.inputFields).trim();
		inputQuery = getParam().getValue(ParamKey.inputQuery, this.inputQuery).trim();
		inputSort = getParam().getValue(ParamKey.inputSort, this.inputSort).trim();
		inputLimit = getParam().getIntValue(ParamKey.inputLimit);
		needSplit = getParam().getBooleanValue(ParamKey.needSplit, this.needSplit);
		splitKeyPattern = getParam().getValue(ParamKey.splitKeyPattern, this.splitKeyPattern);
		splitSize = getParam().getIntValue(ParamKey.splitSize, this.splitSize);
		fieldNeedSplit = getParam().getBooleanValue(ParamKey.fieldNeedSplit, false);
		fieldSplitChar = getParam().getCharValue(ParamKey.filedSplitChar, (char)0);
		dataTransform = getParam().getValue(ParamKey.dataTransformClass, "");

		log.info("inputUri:" + inputUri);
		uri = new MongoURI(inputUri);
		fields = MongoUtils.convertStringToDBObject(inputFields);
		query = MongoUtils.convertStringToDBObject(inputQuery);
		sort = MongoUtils.convertStringToDBObject(inputSort);
		
		
		if (null == fields){
			throw new IllegalArgumentException("ParamKey inputFields can't not be empty. ");
		}else{
			fieldsSet = fields.keySet();
			allFieldsOrNot = false;
			log.info("fieldsSet contains:" + Joiner.on('\t').join(fieldsSet));
		}
	}
	
	@SuppressWarnings("deprecation")
	@Override
	public void connection() {
		if (cursor == null){
			cursor = MongoUtils.getCollection(uri).find(query, fields);
			if (null != sort){
				cursor = cursor.sort(sort);
			}
			if (inputLimit > 0){
				cursor = cursor.limit(inputLimit);
			}
			cursor.slaveOk();
			log.info("cursor:" + cursor.toString());
			
		}else{
			log.info("cursor is not null.");
		}
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public void read(ILineSender lineSender) {
		ITransformer transformer = null;
		if(!dataTransform.isEmpty()) {
			transformer = TransformerFactory.create(dataTransform);
		}

		String item = null;
		while(cursor.hasNext()){
			ILine line = lineSender.createNewLine();
			current = cursor.next();
			log.debug("current cursor object:" + current);
			
			if (!allFieldsOrNot){
				for(String field : fieldsSet){
					item =  String.valueOf(current.get(field));
					if(fieldNeedSplit && fieldsSet.size()==1) {
						int start = 0;
						int end = item.indexOf(fieldSplitChar);
						while(end!=-1) {
							String fieldStr = null;
							if(start != end) {
								fieldStr = item.substring(start,end);
							}
							line.addField(fieldStr);
							start = end + 1;
							end = item.indexOf(fieldSplitChar,start);
						}
						if(start == item.length()) {
							line.addField(null);
						}else {
							line.addField(item.substring(start));
						}
					}else if (null == item){
						line.addField(null);
					}else {
						line.addField(item);
					}
				}
			}else{
				log.info("Using all fields strategy, it is not appreciable. ");
				Map<String, BSONObject> keys = current.toMap();
				for (Map.Entry<String, BSONObject> entry : keys.entrySet()) {
					line.addField(entry.getValue().toString());
				}
				
			}
			if(!dataTransform.isEmpty()) {
				line = transformer.transform(line);
			}
			log.debug("send line to bufferLineExchanger:" + line.toString('\t'));
			if (lineSender.send(line)){
				getMonitor().increaseSuccessLines();
			}else{
				getMonitor().increaseFailedLines();
			}
		}
		lineSender.flush();
	}
	@Override
	public void finish() {
		if (cursor != null){
            cursor.close();
		}
	}
}
