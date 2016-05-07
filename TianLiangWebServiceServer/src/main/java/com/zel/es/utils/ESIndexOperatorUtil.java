package com.zel.es.utils;

import io.searchbox.core.Bulk;
import io.searchbox.core.Index;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.DeleteIndex;

import java.net.SocketTimeoutException;
import java.util.List;
import java.util.Map;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import com.zel.es.manager.es.client.ESClientManager;
import com.zel.es.pojos.ESJestClientPojo;
import com.zel.es.pojos.ESSelfClientPojo;

/**
 * ES索此操作工具类
 * 
 * @author zel
 * 
 */
public class ESIndexOperatorUtil {
	private static MyLogger logger = new MyLogger(ESIndexOperatorUtil.class);

	public ESIndexOperatorUtil() {

	}

	private ESClientManager esClientManager = new ESClientManager();

	/**
	 * 根据indexName创建索引
	 * 
	 * @param indexName
	 * @throws Exception
	 */
	public void createIndex(String indexName) {
		ESJestClientPojo esJestClientPojo = esClientManager.getJestClient();
		try {
			// new Index.Builder(source)
			esJestClientPojo.getJestClient().execute(
					new CreateIndex.Builder(indexName).build());
			logger.info(esJestClientPojo.getPrefix_sign()
					+ "create index successful!");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			esClientManager.pushJestClient(esJestClientPojo);
		}
	}

	public void createIndex(String indexName, String indexType) {
		ESJestClientPojo esJestClientPojo = esClientManager.getJestClient();
		try {
			esJestClientPojo.getJestClient().execute(
					new CreateIndex.Builder(indexName).build());
			logger.info(esJestClientPojo.getPrefix_sign() + "index ok!");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			esClientManager.pushJestClient(esJestClientPojo);
		}
	}

	public void createIndexWithESClient(String indexName, String indexType,
			String mappingJsonSource) {
		ESSelfClientPojo esClientPojo = esClientManager.getESClient();
		try {
			// esClientPojo.getEsClient().admin().indices()
			// .prepareCreate(indexName).execute().actionGet();
			esClientPojo.getEsClient().admin().indices()
					.preparePutMapping(indexName).setType(indexType)
					.setSource(mappingJsonSource).execute().actionGet();
			logger.info(esClientPojo.getPrefix_sign()
					+ "create index and type by es client!");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			esClientManager.pushESClient(esClientPojo);
		}
	}

	// 增加单一对象数据加进ES索引中
	public void addIndexToES(Index index) {
		ESJestClientPojo esJestClientPojo = esClientManager.getJestClient();
		// 遇到time out时，进行重新请求
		int repeat_time = 0;
		while (repeat_time < SystemParas.es_index_fail_max_time) {
			try {
				esJestClientPojo.getJestClient().execute(index);
				logger.info(esJestClientPojo.getPrefix_sign() + "index ok!");
				break;
			} catch (SocketTimeoutException timeout) {
				repeat_time++;
				logger.info(esJestClientPojo.getPrefix_sign()
						+ "occur timeout when indexing,will index try again!");
				try {
					Thread.sleep(SystemParas.es_index_fail_waitting_time);
				} catch (Exception e) {
					e.printStackTrace();
				}
			} catch (Exception e) {
				repeat_time++;
				logger.info(esJestClientPojo.getPrefix_sign()
						+ "occur exception when indexing,will index try again!");
				e.printStackTrace();
				try {
					Thread.sleep(SystemParas.es_index_fail_waitting_time);
				} catch (Exception sleepException) {
					sleepException.printStackTrace();
				}
			}
		}
		esClientManager.pushJestClient(esJestClientPojo);
	}

	// 批量增加对象集合数据加进ES索引中,indexName为索引名称，typeName为索引类型，indexList为批量可索引对象集合
	public void addIndexToES(String indexName, String typeName,
			List<Index> indexList) {
		if (indexList == null || indexList.isEmpty()) {
			return;
		}
		// 通过manager类得到jest client
		ESJestClientPojo esJestClientPojo = esClientManager.getJestClient();
		// 构建索引名称、索引类型的builder，此为加入索引数据的前提条件
		Bulk.Builder builder = new Bulk.Builder().defaultIndex(indexName)
				.defaultType(typeName);
		// 每个index就是一个可索引对象，将其批量构建成json串数据
		builder.addAction(indexList).build();
		// 构造进可执行实质索引操作的批量操作工具类中
		Bulk bulk2 = new Bulk(builder);

		// 遇到time out时，进行重新请求
		int repeat_time = 0;
		// int continious_exception_count = 0;
		while (repeat_time <= SystemParas.es_index_fail_max_time) {
			try {
				// 实质执行将数据转化为成索引的索引操作
				esJestClientPojo.getJestClient().execute(bulk2);
				logger.info(esJestClientPojo.getPrefix_sign() + "index ok!");
				break;
			} catch (SocketTimeoutException timeout) {
				timeout.printStackTrace();
				repeat_time++;
				logger.info(esJestClientPojo.getPrefix_sign()
						+ "occur timeout when indexing,will do it try again!");
				try {
					Thread.sleep(SystemParas.es_index_fail_waitting_time);
				} catch (Exception e) {
					e.printStackTrace();
				}
				// continious_exception_count++;
				// 更换新的jest client
				// if (continious_exception_count ==
				// SystemParas.es_client_continious_exception_count_max) {
				// esJestClientPojo = esClientManager
				// .getNewClient(esJestClientPojo);
				// }
				if (repeat_time == SystemParas.es_index_fail_max_time) {
					esClientManager.removeJestClient(esJestClientPojo);
					logger.info("when indexing occur error,remove a jest client");
					esJestClientPojo = esClientManager.getJestClient();
					repeat_time = 0;
				}
			} catch (Exception e) {
				e.printStackTrace();
				repeat_time++;
				logger.info(esJestClientPojo.getPrefix_sign()
						+ "occur unknown error when indexing,will do it try again!");
				e.printStackTrace();
				try {
					Thread.sleep(SystemParas.es_index_fail_waitting_time);
				} catch (Exception sleepException) {
					sleepException.printStackTrace();
				}
				// continious_exception_count++;
				// 更换新的jest client
				// if (continious_exception_count ==
				// SystemParas.es_client_continious_exception_count_max) {
				// esJestClientPojo = esClientManager
				// .getNewClient(esJestClientPojo);
				// }
				if (repeat_time == SystemParas.es_index_fail_max_time) {
					// esJestClientPojo = esClientManager
					// .getNewClient(esJestClientPojo);
					esClientManager.removeJestClient(esJestClientPojo);
					logger.info("when indexing occur error,remove a jest client");
					esJestClientPojo = esClientManager.getJestClient();
					repeat_time = 0;
				}
			}
			// if (repeat_time >= SystemParas.es_index_fail_max_time) {
			// logger.info("the error index times is to the max try,will abandom this data!");
			// break;
			// }
		}
		esClientManager.pushJestClient(esJestClientPojo);
	}

	// 直接删除指定索引库
	public void deleteByIndexName(String indexName) {
		ESJestClientPojo esJestClientPojo = esClientManager.getJestClient();
		DeleteIndex dIndex = new DeleteIndex(new DeleteIndex.Builder(indexName));
		// 遇到time out时，进行重新请求
		int repeat_time = 0;
		while (repeat_time < SystemParas.es_index_fail_max_time) {
			try {
				esJestClientPojo.getJestClient().execute(dIndex);
				logger.info(esJestClientPojo.getPrefix_sign()
						+ "delete index by indexName successful!");
				break;
			} catch (SocketTimeoutException timeout) {
				repeat_time++;
				logger.info(esJestClientPojo.getPrefix_sign()
						+ "occur timeout when deleteByIndexName,will do it try again!");
				try {
					Thread.sleep(SystemParas.es_index_fail_waitting_time);
				} catch (Exception e) {
					e.printStackTrace();
				}
				continue;
			} catch (Exception e) {
				e.printStackTrace();
				logger.info(esJestClientPojo.getPrefix_sign()
						+ "to add one item index occur error,will jump the item!");
				break;
			}
		}
		esClientManager.pushJestClient(esJestClientPojo);
	}

	/**
	 * 以下是ES端提供的client来操作索引端,通过构建QueryBuilder来达到按指定条件来删除索引
	 * 
	 * @param args
	 */
	public void deleteIndexByQuery(String indexName, String typeName,
			QueryBuilder queryBuilder) {
		ESSelfClientPojo esClientPojo = esClientManager.getESClient();

		// 遇到time out时，进行重新请求
		int repeat_time = 0;
		while (repeat_time < SystemParas.es_index_fail_max_time) {
			try {
				if (typeName == null) {
					esClientPojo.getEsClient().prepareDeleteByQuery(indexName)
							.setQuery(queryBuilder).execute().actionGet();
				} else {
					esClientPojo.getEsClient().prepareDeleteByQuery(indexName)
							.setTypes(typeName).setQuery(queryBuilder)
							.execute().actionGet();
				}
				logger.info(esClientPojo.getPrefix_sign()
						+ "delete index by query successful!");
				break;
			} catch (Exception e) {
				e.printStackTrace();
				logger.info(esClientPojo.getPrefix_sign()
						+ "delete index fail,please try again!");
			}
			repeat_time++;
		}
		esClientManager.pushESClient(esClientPojo);
	}

	// 通过id array去更新索引
	public boolean updateIndexByIds(String indexName, String typeName,
			String id, Map<String, Object> newFieldsMap) {
		ESSelfClientPojo esClientPojo = esClientManager.getESClient();

		// 遇到time out时，进行重新请求
		int repeat_time = 0;
		while (repeat_time < SystemParas.es_index_fail_max_time) {
			try {
				newFieldsMap.put("is_new", 1);

				esClientPojo.getEsClient()
						.prepareUpdate(indexName, typeName, id)
						.setDoc(newFieldsMap).execute().actionGet();
				// Set<Entry<String,Object>> set=newFieldsMap.entrySet();
				// for(Entry<String,Object> entry:set){
				// System.out.println(entry.getKey());
				// System.out.println(entry.getValue());
				// }

				logger.info(esClientPojo.getPrefix_sign() + "," + id
						+ "  update index by id list successful!");
				break;
			} catch (Exception e) {
				e.printStackTrace();
				logger.info(esClientPojo.getPrefix_sign()
						+ "update index by id list fail,please try again!");
			}
			repeat_time++;
		}
		esClientManager.pushESClient(esClientPojo);
		return true;
	}

	public boolean updateIndexByIds4Emotion(String indexName, String typeName,
			String id, Map<String, Object> newFieldsMap) {
		ESSelfClientPojo esClientPojo = esClientManager.getESClient();

		// 遇到time out时，进行重新请求
		int repeat_time = 0;
		while (repeat_time < SystemParas.es_index_fail_max_time) {
			try {
				// newFieldsMap.put("is_new", 1);
				esClientPojo.getEsClient()
						.prepareUpdate(indexName, typeName, id)
						.setDoc(newFieldsMap).execute().actionGet();
				// esClientPojo.getEsClient()
				// .prepareDelete(indexName, typeName, id).execute()
				// .actionGet();
				// Set<Entry<String,Object>> set=newFieldsMap.entrySet();
				// for(Entry<String,Object> entry:set){
				// System.out.println(entry.getKey());
				// System.out.println(entry.getValue());
				// }

				logger.info(esClientPojo.getPrefix_sign() + "," + id
						+ "  update index by id list successful-4 emotion !");
				break;
			} catch (Exception e) {
				e.printStackTrace();
				logger.info(esClientPojo.getPrefix_sign()
						+ "update index by id list fail,please try again-4 emotion !");
			}
			repeat_time++;
		}
		esClientManager.pushESClient(esClientPojo);
		return true;
	}

	/**
	 * 以下是ES端提供的client来操作索引端，删除指定的indexname或是indexname下的名称为type的表数据
	 * 
	 * @param args
	 */
	public void deleteIndexByIndexOrType(String indexName, String typeName) {
		ESSelfClientPojo esClientPojo = esClientManager.getESClient();

		if (StringOperatorUtil.isBlank(indexName)) {
			System.out.println("indexName不可为空!");
		}

		// 遇到time out时，进行重新请求
		int repeat_time = 0;
		while (repeat_time < SystemParas.es_index_fail_max_time) {
			try {
				if (StringOperatorUtil.isNotBlank(typeName)) {
					esClientPojo.getEsClient().prepareDeleteByQuery(indexName)
							.setTypes(typeName)
							.setQuery(QueryBuilders.matchAllQuery()).execute()
							.actionGet();
				} else {
					this.deleteByIndexName(indexName);
				}

				logger.info(esClientPojo.getPrefix_sign()
						+ "delete index by query successful!");

				break;
			} catch (Exception e) {
				e.printStackTrace();
				logger.info(esClientPojo.getPrefix_sign()
						+ "delete index fail,please try again!");
			}
			repeat_time++;
		}
		esClientManager.pushESClient(esClientPojo);
	}

	// 直接删除索引中的某type
	public void dropIndexType(String indexName, String typeName) {
		ESSelfClientPojo esClientPojo = esClientManager.getESClient();

		if (StringOperatorUtil.isBlank(indexName)
				|| StringOperatorUtil.isBlank(typeName)) {
			System.out.println("indexName或typeName不可为空!");
		}

		// 遇到time out时，进行重新请求
		int repeat_time = 0;
		while (repeat_time < SystemParas.es_index_fail_max_time) {
			try {
				esClientPojo.getEsClient().admin().indices()
						.prepareDeleteMapping(indexName).setType(typeName)
						.execute().actionGet();
				logger.info(esClientPojo.getPrefix_sign()
						+ "delete indexType successful!");
				break;
			} catch (Exception e) {
				e.printStackTrace();
				logger.info(esClientPojo.getPrefix_sign()
						+ "delete indexType fail,please try again!");
			}
			repeat_time++;
		}
		esClientManager.pushESClient(esClientPojo);
	}

	public static void main(String[] args) {
		ESIndexOperatorUtil esIndexOperatorUtil = new ESIndexOperatorUtil();
		// String indexName = "yuqing_letv";
		// String indexName = "yuqing_xinlian";
		// String indexName = "conac_yuqing";
		String indexName = "yuqing_studio";

		// String indexType = "cinema_business_per_day_statistics";
		// String indexType = "movie_meta_desc";
		// String jsonFilePath = "table_schema/movie_meta_desc.json";
		// String indexType = "movie_statistic_info";
		// String jsonFilePath = "table_schema/movie_statistic_info.json";
		// indexType="cinema_business";
		// indexType="movie_basic_info";
		// indexType = "view_expect_people_number";
		// String jsonFilePath = "cinema_business_per_day_statistics.json";
		// jsonFilePath="cinema_business.json";
		// jsonFilePath="movie_basic_info.json";
		// jsonFilePath = "view_expect_people_number.json";

		// indexType = "cinema_business_per_day_statistics";

		// String indexType = "portals_web_data";
		// String jsonFilePath = "table_schema/portals_web_data.json";

		// String indexType = "portals_web_data";
		// String jsonFilePath = "table_schema/portals_web_data.json";

		// 新浪微博--博文
		// String indexType = "sns_weibo_sina_doc";
		// String jsonFilePath = "table_schema/sns/sns_weibo_sina_doc.json";

		// 新浪微博--博主scheme
		String indexType = "portals_web_data";
		String jsonFilePath = "table_schema/news_webpage/portals_web_data.json";

		// 新浪微博--博主
		// String indexType = "sns_weibo_sina_person";
		// String jsonFilePath = "table_schema/sns/sns_weibo_sina_person.json";

		// indexType="movie_index";
		// indexType="cinema_business";
		// indexType="movie_basic_info";
		// indexType="view_expect_people_number";
		// jsonFilePath="cinema_business_per_day_statistics.json";
		// jsonFilePath="cinema_business.json";
		// jsonFilePath="movie_basic_info.json";
		// jsonFilePath="movie_index.json";

		String mappingJsonSource = IOUtil.readFile(jsonFilePath,
				StaticValue.default_encoding);

		// 删除索引
		// esIndexOperatorUtil.deleteByIndexName(indexName);
		// esIndexOperatorUtil.dropIndexType(indexName, indexType);
		// 创建索引
		esIndexOperatorUtil.createIndex(indexName);
		esIndexOperatorUtil.createIndexWithESClient(indexName, indexType,
				mappingJsonSource);
	}
}
