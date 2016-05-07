package com.zel.impl.ws.es.index;

import java.util.HashMap;
import java.util.List;

import javax.jws.WebService;

import com.bigdata.entity.CinemaBusinessEntity;
import com.bigdata.entity.CinemaBusinessPerDayStatisticsEntity;
import com.bigdata.entity.MovieBasicInfoEntity;
import com.bigdata.entity.MovieBusinessEntity;
import com.bigdata.entity.MovieBusinessPerDayStatisticsEntity;
import com.bigdata.entity.ScreenBusinessEntity;
import com.bigdata.entity.ScreenBusinessPerDayStatisticsEntity;
import com.bigdata.entity.index.IndexEntity;
import com.bigdata.entity.viewexpect.ViewExpectEntity;
import com.zel.es.manager.es.ESIndexManager;
import com.zel.es.pojos.index.CrawlData4LastCommentDesc;
import com.zel.es.pojos.index.CrawlData4MovieCommentDecPojo;
import com.zel.es.pojos.index.CrawlData4MovieMetaDescPojo;
import com.zel.es.pojos.index.CrawlData4MovieStatisticInfoPojo;
import com.zel.es.pojos.index.CrawlData4PortalSite;
import com.zel.es.pojos.index.CrawlData4SnsWeiboDoc;
import com.zel.es.pojos.index.CrawlData4SnsWeiboPerson;
import com.zel.es.pojos.index.CrawlData4bbs;
import com.zel.es.pojos.search.SearchConditionPojo;
import com.zel.es.utils.MyLogger;
import com.zel.iface.ws.es.index.IEsIndexService;

/**
 * 任务服务实现类
 * 
 * @author zel
 * 
 */
@WebService(endpointInterface = "com.zel.iface.ws.es.index.IEsIndexService", serviceName = "EsIndexService")
public class EsIndexServiceImpl implements IEsIndexService {
	@Override
	public void addBatchIndex4RandomObject(String indexName, String indexType,
			List<ScreenBusinessEntity> pojoList) {
		// TODO Auto-generated method stub
		ESIndexManager.addBatchIndexByObj(indexName, indexType, pojoList);
	}
	

	
	//院线票房数据
	public void addBatchIndex4Screens(String indexName, String indexType,
			List<ScreenBusinessEntity> pojoList)
			{
		ESIndexManager.addBatchIndexByObj(indexName, indexType, pojoList);
			}
	
	//院线每天统计结果
	public void addBatchIndex4ScreensPerDay(String indexName, String indexType,
			List<ScreenBusinessPerDayStatisticsEntity> pojoList)
			{
		ESIndexManager.addBatchIndexByObj(indexName, indexType, pojoList);
			}
	

	MyLogger logger = new MyLogger(EsIndexServiceImpl.class);

	@Override
	public void updateEmotionIndex4WebDoc(
			SearchConditionPojo searchConditionPojo, String emotion_key,
			Object emotion_value) {
		// TODO Auto-generated method stub
		HashMap<String, Object> newFieldsMap = new HashMap<String, Object>();
		newFieldsMap.put(emotion_key, emotion_value);
		ESIndexManager.updateEmotionIndex4WebDoc(searchConditionPojo,newFieldsMap);
	}

	@Override
	public void updateAndSaveIndex4WeiboDoc(
			SearchConditionPojo searchConditionPojo,
			HashMap<String, Object> newFieldsMap,
			List<CrawlData4SnsWeiboDoc> pojoList) {
		// TODO Auto-generated method stub
		ESIndexManager.updateAndSaveIndex(searchConditionPojo, pojoList,
				newFieldsMap);
	}

	@Override
	public void updateAndSaveIndex4WeiboPerson(
			SearchConditionPojo searchConditionPojo,
			HashMap<String, Object> newFieldsMap,
			List<CrawlData4SnsWeiboPerson> pojoList) {
		// TODO Auto-generated method stub
		ESIndexManager.updateAndSaveIndex(searchConditionPojo, pojoList,
				newFieldsMap);
	}

	@Override
	public String test() {
		// TODO Auto-generated method stub
		System.out.println("it is here!");
		return null;
	}

	/**
	 * 批量添加索引
	 */
	@Override
	public void addBatchIndex(String indexName, String indexType,
			List<String> pojoList) {
		// TODO Auto-generated method stub
		// ESIndexManager.addBatchIndex(indexName, indexType, pojoList);
	}

	/**
	 * 暂时留用indexType
	 */
	@Override
	public void createIndex(String indexName, String indexType) {
		// TODO Auto-generated method stub
		ESIndexManager.createIndex(indexName, indexType);
	}

	@Override
	public void addBatchIndexByString(String indexName, String indexType,
			List<String> lineString) {
		// TODO Auto-generated method stub
		ESIndexManager.addBatchIndexByString(indexName, indexType, lineString);
	}

	@Override
	public void addBatchIndexWeiboPerson(String indexName, String indexType,
			List<CrawlData4SnsWeiboPerson> pojoList) {
		ESIndexManager.addBatchIndexByObj(indexName, indexType, pojoList);
	}

	@Override
	public void addBatchIndex4WeiboDoc(String indexName, String indexType,
			List<CrawlData4SnsWeiboDoc> pojoList) {
		// TODO Auto-generated method stub
		ESIndexManager.addBatchIndexByObj(indexName, indexType, pojoList);
		// System.out.println("test save doc successful!");
	}

	@Override
	public void addBatchIndex4BBS(String indexName, String indexType,
			List pojoList) {
		// TODO Auto-generated method stub
		ESIndexManager.addBatchIndexByObj(indexName, indexType, pojoList);
	}
	
	
	@Override
public void addBatchIndex4MovieIndex(String indexName, String indexType,
List<IndexEntity> pojoList) {
// TODO Auto-generated method stub
ESIndexManager.addBatchIndexByObj(indexName, indexType, pojoList);
}
	
	@Override
	public void addBatchIndex4PortalWeb(String indexName, String indexType,
			List pojoList) {
		// TODO Auto-generated method stub
		ESIndexManager.addBatchIndexByObj(indexName, indexType, pojoList);
	}

	@Override
	public void addBatchIndex4Movie(String indexName, String indexType,
			List<MovieBusinessEntity> pojoList) {
		ESIndexManager.addBatchIndexByObj(indexName, indexType, pojoList);		
	}
	
	@Override
	public void addBatchIndex4Cinema(String indexName, String indexType,
			List<CinemaBusinessEntity> pojoList) {
		ESIndexManager.addBatchIndexByObj(indexName, indexType, pojoList);		
	}
	
	@Override
	public void addBatchIndex4MovieBasicInfo(String indexName, String indexType,
			List<MovieBasicInfoEntity> pojoList) {
		ESIndexManager.addBatchIndexByObj(indexName, indexType, pojoList);		
	}


	@Override
	public void addBatchIndex4MoviePerDay(String indexName, String indexType,
			List<MovieBusinessPerDayStatisticsEntity> pojoList) {
		ESIndexManager.addBatchIndexByObj(indexName, indexType, pojoList);		
	}
	
	@Override
	public void addBatchIndex4CinemaPerDay(String indexName, String indexType,
			List<CinemaBusinessPerDayStatisticsEntity> pojoList) {
		ESIndexManager.addBatchIndexByObj(indexName, indexType, pojoList);		
	}



	@Override
	public void addBatchIndex4MovieMetaDesc(String indexName, String indexType,
			List<CrawlData4MovieMetaDescPojo> pojoList) {
		ESIndexManager.addBatchIndexByObj(indexName, indexType, pojoList);
		
	}
	
	@Override
	public void addBatchIndex4LastCommentDesc(String indexName, String indexType,
			List<CrawlData4LastCommentDesc> pojoList) {
		ESIndexManager.addBatchIndexByObj(indexName, indexType, pojoList);
		
	}
	
	@Override
	public void addBatchIndex4MovieCommentDec(String indexName, String indexType,
			List<CrawlData4MovieCommentDecPojo> pojoList) {
		ESIndexManager.addBatchIndexByObj(indexName, indexType, pojoList);
		
	}
	
	@Override
	public void addBatchIndex4MovieStatisticInfo(String indexName, String indexType,
			List<CrawlData4MovieStatisticInfoPojo> pojoList) {
		ESIndexManager.addBatchIndexByObj(indexName, indexType, pojoList);
		
	}
	
	
	@Override
	public void addBatchIndex4ViewExpect(String indexName, String indexType,
			List<ViewExpectEntity> pojoList) {
		ESIndexManager.addBatchIndexByObj(indexName, indexType, pojoList);
		
	}
}
