package com.dp.nebula.wormhole.transform.impls;

//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;

import com.dp.nebula.wormhole.common.AbstractTransformer;
import com.dp.nebula.wormhole.common.DefaultLine;
import com.dp.nebula.wormhole.common.interfaces.ILine;

public class AddAndFiltTransformer extends AbstractTransformer{
//	private final Log s_logger = LogFactory.getLog(HippoMongoToGPTransformer.class);
	
	@Override
	public ILine transform(ILine line, String params) {
		ILine result = new DefaultLine();
		String [] idStrs = params.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
		for(String idStr:idStrs) {
			int id = 0;
			try{
				id = Integer.parseInt(idStr);
			} catch(NumberFormatException e){
				if(idStr.length()>=2 && idStr.startsWith("\"") && idStr.endsWith("\"")) {
					result.addField(idStr.substring(1,idStr.length()-1));
				}
				continue;
			}
			result.addField(line.getField(id));
		}
		return result;
	}
	
	@Override
	public ILine transform(ILine line) {
		ILine result = new DefaultLine();
		result.addField("good");	
		return result;
	}
	

}
