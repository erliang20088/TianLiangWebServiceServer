package com.zel.impl.ws.es.search;

import javax.jws.WebService;

import com.zel.es.manager.es.ESSearchManager;
import com.zel.es.pojos.search.SearchConditionPojo;
import com.zel.es.utils.MyLogger;
import com.zel.iface.ws.es.search.IEsSearchService;

/**
 * 任务服务实现类
 * 
 * @author zel
 * 
 */
@WebService(endpointInterface = "com.zel.iface.ws.es.search.IEsSearchService", serviceName = "EsSearchService")
public class EsSearchServiceImpl implements IEsSearchService {
	MyLogger logger = new MyLogger(EsSearchServiceImpl.class);

	@Override
	public String test() {
		// TODO Auto-generated method stub
		System.out.println("it is here!");
		return null;
	}

	@Override
	public String search4RetJson(SearchConditionPojo searchConditionPojo) {
		// TODO Auto-generated method stub
		return ESSearchManager.search4RetJson(searchConditionPojo);
	}

//	@Override
//	public List<Object> search4RetObj(
//			SearchConditionPojo searchConditionPojo) {
//		// TODO Auto-generated method stub
//		return ESSearchManager.search4RetObj(searchConditionPojo);
//		// return null;
//	}
}
