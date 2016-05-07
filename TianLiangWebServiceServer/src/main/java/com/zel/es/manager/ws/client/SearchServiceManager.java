package com.zel.es.manager.ws.client;

import io.searchbox.core.SearchResult;

import java.util.List;

import com.zel.iface.ws.es.search.IEsSearchService;

/**
 * 对searchservice调用的管理类
 * 
 * @author zel
 * 
 */
public class SearchServiceManager {
	private static ServiceClientPool<IEsSearchService> serviceClientPool = new ServiceClientPool<IEsSearchService>();;
	// 初始化连接池
	static {
		for (int i = 0; i < 5; i++) {
			serviceClientPool
					.addServiceClient(createSearchService(IEsSearchService.class));
		}
	}

	public static IEsSearchService createSearchService(Class serviceClass) {
		// JaxWsProxyFactoryBean soapFactoryBean = new JaxWsProxyFactoryBean();
		// soapFactoryBean.setAddress("http://" + "localhost" + ":"
		// + 9999 + "/"
		// + "EsSearchService");
		// soapFactoryBean.setServiceClass(serviceClass);
		// return (IEsSearchService) soapFactoryBean.create();
		return null;
	}

	// 向ws发送检索人物信息的条件
	public List<SearchResult.Hit<Object, Void>> searchAdCrowdBaseInfo() {
		IEsSearchService searchService = serviceClientPool
				.popIdleServiceClient();

		try {
			searchService.test();
			;
		} finally {
			// 将用完的ws search client放回连接池
			serviceClientPool.pushToIdleServicePool(searchService);
		}
		return null;
	}
}
