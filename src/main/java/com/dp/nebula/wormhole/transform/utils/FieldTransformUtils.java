package com.dp.nebula.wormhole.transform.utils;

import java.text.SimpleDateFormat;
import java.util.Date;

public final class FieldTransformUtils {
	
	private FieldTransformUtils () {
		
	}
	
	public static String fromUnixTime(long unixTime){
		Date dateTime = new Date(unixTime*1000);
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return dateFormat.format(dateTime);
	}
	
}
