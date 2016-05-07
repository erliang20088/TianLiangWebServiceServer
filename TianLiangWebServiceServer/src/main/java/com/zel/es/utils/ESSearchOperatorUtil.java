package com.zel.es.utils;

import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.SearchResult.Hit;

import java.net.SocketTimeoutException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.elasticsearch.search.builder.SearchSourceBuilder;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.zel.es.manager.es.client.ESClientManager;
import com.zel.es.pojos.ESJestClientPojo;

/**
 * ES搜索操作工具类
 * 
 * @author zel
 * 
 */
public class ESSearchOperatorUtil<T> {
	private static MyLogger logger = new MyLogger(ESSearchOperatorUtil.class);

	private static ESClientManager esClientManager = new ESClientManager();

	public ESSearchOperatorUtil() {

	}

	// 由SearchSourceBuilder得到相应的搜索结果
	private static SearchResult search(String indexName, String indexType,
			SearchSourceBuilder searchSourceBuilder) {
		ESJestClientPojo esJestClientPojo = esClientManager.getJestClient();
		Search search = null;
		String json_result = searchSourceBuilder.toString();
		if (StringOperatorUtil.isBlank(indexType)) {
			search = new Search.Builder(json_result).addIndex(indexName)
					.build();
		} else {
			search = new Search.Builder(json_result).addIndex(indexName)
					.addType(indexType).build();
		}

		// 遇到time out时，进行重新请求
		int repeat_time = 0;
		// int continious_exception_count = 0;
		SearchResult searchResult = null;
		while (repeat_time < SystemParas.es_index_fail_max_time) {
			try {
				searchResult = esJestClientPojo.getJestClient().execute(search);
				break;
			} catch (SocketTimeoutException timeout) {
				repeat_time++;
				timeout.printStackTrace();
				logger.info(esJestClientPojo.getPrefix_sign()
						+ "occur timeout when searching,will search try again!");
				try {
					Thread.sleep(SystemParas.es_index_fail_waitting_time);
				} catch (Exception e) {
					e.printStackTrace();
				}
				// continious_exception_count++;
				// 更换新的jest client
				// if (continious_exception_count ==
				// SystemParas.es_client_continious_exception_count_max) {
				if (repeat_time == SystemParas.es_index_fail_max_time) {
					// esJestClientPojo = esClientManager
					// .getNewClient(esJestClientPojo);
					esClientManager.removeJestClient(esJestClientPojo);
					logger.info("when search occur error,remove a jest client");
					esJestClientPojo = esClientManager.getJestClient();
					repeat_time = 0;
				}
				// continue;
			} catch (Exception e) {
				repeat_time++;
				e.printStackTrace();
				logger.info(esJestClientPojo.getPrefix_sign()
						+ "occur exception when searching,will search try again!");
				try {
					Thread.sleep(SystemParas.es_index_fail_waitting_time);
				} catch (Exception sleepException) {
					sleepException.printStackTrace();
				}
				if (repeat_time == SystemParas.es_index_fail_max_time) {
					// esJestClientPojo = esClientManager
					// .getNewClient(esJestClientPojo);
					esClientManager.removeJestClient(esJestClientPojo);
					logger.info("when search occur error,remove a jest client");
					esJestClientPojo = esClientManager.getJestClient();
					repeat_time = 0;
				}
			}
		}
		esClientManager.pushJestClient(esJestClientPojo);

		return searchResult;
	}

	// 通过搜索返回结果，以对象形式
	public List<T> search4RetObj(String indexName, String indexType,
			SearchSourceBuilder searchSourceBuilder, Class<T> resultClass) {
		SearchResult searchResult = search(indexName, indexType,
				searchSourceBuilder);
		if (searchResult == null) {
			return null;
		}
		List<SearchResult.Hit<T, Void>> hits = searchResult
				.getHits(resultClass);
		List<T> resultClassList = new LinkedList<T>();

		Iterator<Hit<T, Void>> hitsList = hits.iterator();
		while (hitsList.hasNext()) {
			resultClassList.add(hitsList.next().source);
		}
		return resultClassList;
	}

	// 通过搜索返回结果，以json串形式
	public String search4RetJson(String indexName, String indexType,
			SearchSourceBuilder searchSourceBuilder) {
		SearchResult searchResult = search(indexName, indexType,
				searchSourceBuilder);
		if (searchResult == null) {
			return null;
		}
		String jsonResult = searchResult.getJsonString();

		return jsonResult;
	}

	// 通过搜索返回结果，返回_id的列表
	public List<String> search4IndexIdList(String indexName, String indexType,
			SearchSourceBuilder searchSourceBuilder) {
		SearchResult searchResult = search(indexName, indexType,
				searchSourceBuilder);
		if (searchResult == null) {
			return null;
		}
		JsonObject jsonObj = searchResult.getJsonObject();
		jsonObj = jsonObj.get("hits").getAsJsonObject();
		List<String> resultList = null;
		if (jsonObj.has("hits")) {
			resultList = new LinkedList<String>();
			JsonArray jsonArray = jsonObj.get("hits").getAsJsonArray();
			for (JsonElement obj : jsonArray) {
				JsonObject temp_jsonObj = obj.getAsJsonObject();
				if (temp_jsonObj.has("_id")) {
					resultList.add(temp_jsonObj.get("_id").getAsString());
				}
			}
		}
		return resultList;
	}

	public static void main(String[] args) {

	}
}
