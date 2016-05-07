package com.zel.es.utils;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * gson操作工具类
 * 
 * @author zel
 * 
 */
public class GsonOperatorUtil {
	/**
	 * 将一个json串转换为Gson的JsonObject
	 * 
	 * @param jsonStr
	 * @return
	 */
	public static JsonObject parse(String jsonStr) {
		JsonParser jsonParser = new JsonParser();
		JsonObject jsonObj = (JsonObject) jsonParser.parse(jsonStr);
		
		return jsonObj;
	}
}
