package com.zel.test;

import io.searchbox.core.Index;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.SearchResult.Hit;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.DeleteIndex;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import com.zel.es.manager.es.ESIndexManager;
import com.zel.es.manager.es.client.ESClientManager;
import com.zel.es.manager.es.index.mq.IndexMessageQueueManager;
import com.zel.es.pojos.ESJestClientPojo;
import com.zel.es.pojos.index.CrawlData4PortalSite;
import com.zel.es.pojos.search.SearchConditionItem;
import com.zel.es.pojos.search.SearchConditionPojo;

public class MainTestES {
	private static ESClientManager esClientManager = new ESClientManager();

	public static void createIndex(String indexName) throws Exception {
		ESJestClientPojo esJestClientPojo = esClientManager.getJestClient();

		esJestClientPojo.getJestClient().execute(
				new CreateIndex.Builder(indexName).build());
		// esJestClientPojo.getJestClient().shutdownClient();
	}

	// 增加单一对象数据加进ES索引中
	public static void addOneIndexToES(String indexName, String typeName)
			throws Exception {
		ESJestClientPojo esJestClientPojo = esClientManager.getJestClient();

		CrawlData4PortalSite crawlData4PortalSite = new CrawlData4PortalSite();
		crawlData4PortalSite.setUrl("http://www.baidu.com/2");
		crawlData4PortalSite.setTitle("新浪新闻");
		crawlData4PortalSite.setMedia_type(1);
		crawlData4PortalSite.setTransmit_number(3);
		crawlData4PortalSite.setIs_new(0);
		Index index = new Index.Builder(crawlData4PortalSite).index(indexName)
				.type(typeName).build();

		esJestClientPojo.getJestClient().execute(index);
	}

	public static void addBatchObjectsToES() throws Exception {
		// JestClient jestClient = ESCommonOperatorUtil.getJestClient();
		//
		// ArticlePojo articlePojo = new ArticlePojo();
		// articlePojo.setTitle("bulk_3");
		// articlePojo.setContent("value_3");
		// Index index = new Index.Builder(articlePojo).build();
		//
		// articlePojo = new ArticlePojo();
		// articlePojo.setTitle("bulk_4");
		// articlePojo.setContent("value_4");
		// Index index2 = new Index.Builder(articlePojo).build();
		//
		// List<Index> list = new ArrayList<Index>();
		// list.add(index);
		// list.add(index2);
		//
		// Bulk.Builder builder = new Bulk.Builder().defaultIndex("articles")
		// .defaultType("article");
		// builder.addAction(list).build();
		// Bulk bulk2 = new Bulk(builder);
		//
		// jestClient.execute(bulk2);
		//
		// jestClient.shutdownClient();
		// System.out.println(jestClient);
	}

	public static void deleteByIndexName(String indexName) throws Exception {
		ESJestClientPojo esJestClientPojo = esClientManager.getJestClient();
		DeleteIndex dIndex = new DeleteIndex(new DeleteIndex.Builder(indexName));
		esJestClientPojo.getJestClient().execute(dIndex);

		// esJestClientPojo.getJestClient().shutdownClient();
	}

	public static void searchFirst(String indexName, String column, Object query)
			throws Exception {
		ESJestClientPojo esJestClientPojo = esClientManager.getJestClient();

		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

		// searchSourceBuilder.query(QueryBuilders.boolQuery()
		// .must(QueryBuilders.termQuery(column, query))
		// .must(QueryBuilders.termQuery("ad_source", 0)));
		// searchSourceBuilder
		// .query(QueryBuilders.rangeQuery("ts")
		// .from(new Date().getTime() - 14 * 60 * 60 * 1000)
		// .to(new Date().getTime()).includeLower(true)
		// .includeUpper(true));

		searchSourceBuilder.query(QueryBuilders.termQuery(column, query));
		searchSourceBuilder.query(QueryBuilders.rangeQuery(column)
				.from(1401290494288l).to(1401290494288l).includeLower(true)
				.includeUpper(true));

		searchSourceBuilder.from(0);
		searchSourceBuilder.size(5);

		// System.out.println("searchSourceBuilder.toString---"
		// + searchSourceBuilder.toString());
		Search search = new Search.Builder(searchSourceBuilder.toString())
				.addIndex(indexName).build();

		SearchResult searchResult = esJestClientPojo.getJestClient().execute(
				search);

		List<SearchResult.Hit<Object, Void>> hits = searchResult
				.getHits(Object.class);

		// JsonObject jsonObj=searchResult.getJsonObject();
		// String jsonObj = searchResult.getJsonString();
		// System.out.println("jsonObj---" + jsonObj);

		System.out.println(hits.size());
		for (Hit pojo : hits) {
			System.out.println(pojo.source);
		}

		esJestClientPojo.getJestClient().shutdownClient();
	}

	public static void bulkDataToIndex(String indexName, int batch_number,
			int batch_size) throws Exception {
		ESJestClientPojo esJestClientPojo = esClientManager.getJestClient();

		int begin = 0;

		Index index2 = null;
		List<Index> list = new ArrayList<Index>();

		// List<String> strList=new LinkedList<String>();
		StringBuilder sb = new StringBuilder();

		// AdCrowdBaseInfoPojo adCrowdBaseInfoPojo2 = new AdCrowdBaseInfoPojo();
		// for (int i = 0; i < batch_number; i++) {
		// list.clear();
		// for (begin = 0; begin < batch_size; begin++) {
		// adCrowdBaseInfoPojo2
		// .setAd_acct("axcxaasdkfalkf123343.xldkfladf" + begin);
		// adCrowdBaseInfoPojo2.setAd_id("2202022022" + begin);
		// adCrowdBaseInfoPojo2.setAd_source(0);
		// adCrowdBaseInfoPojo2.setCrowd_id(1 + begin);
		// adCrowdBaseInfoPojo2.setDt(201410503 + begin);
		// adCrowdBaseInfoPojo2.setHost("www.2baidu1.com" + begin);
		// adCrowdBaseInfoPojo2.setHost_type("S1");
		// adCrowdBaseInfoPojo2.setMatch_content("11左旋肉碱减肥效果真么样" + begin);
		// adCrowdBaseInfoPojo2.setMatch_type("keyword232");
		// adCrowdBaseInfoPojo2.setSrc_ip("111.1112.111.1211" + begin);
		// adCrowdBaseInfoPojo2.setTs(new Date().getTime());
		//
		// // index2 = new Index.Builder(adCrowdBaseInfoPojo2).build();
		//
		// // list.add(index2);
		// sb.append(adCrowdBaseInfoPojo2.toTabString()
		// + StaticValue.separator_next_line);
		// }

		// IOUtil.writeFile("kk.txt", sb.toString(),
		// StaticValue.default_encoding);
		// }

		esJestClientPojo.getJestClient().shutdownClient();
	}

	public static void main(String[] args) throws Exception {
		// JestClient jestClient = ESCommonOperatorUtil.getJestClient();
		String indexName = "yuqing_letv";
		// String indexName = "portals_web_data";
		// String type_name = "sns_weibo_sina_person";
		// indexName = "tanx_bid";
		// addOneIndexToES("skylight", "index_type");

		// test add data
		// addOneIndexToES(indexName, "portals_web_data");
		// if (true) {
		// return;
		// }
		if (args == null || args.length == 0) {
			// System.out.println("para==null的情况已注释!");
			// deleteByIndexName(indexName);
			// createIndex(indexName);
		} else {
			IndexMessageQueueManager indexMessageQueueManager = null;
			ESIndexManager esIndexManager = new ESIndexManager(
					indexMessageQueueManager);
			// 为删除掉publish_time_long的记录条数
			SearchConditionPojo searchConditionPojo = new SearchConditionPojo();
			searchConditionPojo.setIndexName(indexName);
			// searchConditionPojo.setIndexType("ad_crowd");
			List<SearchConditionItem> searchConditionList = new ArrayList<SearchConditionItem>();
			SearchConditionItem searchConditionItem = new SearchConditionItem();
			searchConditionItem.setName("transmit_number");
			searchConditionItem.setValue(2);
			searchConditionList.add(searchConditionItem);
			searchConditionPojo.setSearchConditionItemList(searchConditionList);
			esIndexManager.deleteByQuery(searchConditionPojo);
			// System.out.println("done");
		}

		// long begin = System.currentTimeMillis();
		// bulkDataToIndex(indexName, 1, 1000);
		// long end = System.currentTimeMillis();
		// System.out.println("共用时---" + (end - begin));

		// Thread.sleep(10000);

		System.out.println("执行完成!");
	}
}
