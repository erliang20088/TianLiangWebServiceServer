package com.zel.impl.ws.config;

import javax.jws.WebService;

import com.zel.es.manager.es.ESIndexManager;
import com.zel.es.pojos.search.SearchConditionPojo;
import com.zel.es.utils.MyLogger;
import com.zel.iface.ws.config.IEsSystemService;

/**
 * 系统操作实现类
 * 
 * @author zel
 * 
 */
@WebService(endpointInterface = "com.zel.iface.ws.config.IEsSystemService", serviceName = "EsSystemService")
public class EsSystemServiceImpl implements IEsSystemService {
	MyLogger logger = new MyLogger(EsSystemServiceImpl.class);

	@Override
	public String test() {
		// TODO Auto-generated method stub
		System.out.println("it is here!");
		return null;
	}

	@Override
	public void deleteIndex(String indexName) {
		// TODO Auto-generated method stub
		ESIndexManager.deleteByIndexName(indexName);
	}

	@Override
	public void deleteIndexByIndexOrType(String indexName, String typeName) {
		ESIndexManager.deleteIndexByIndexOrType(indexName, typeName);
	}

	@Override
	public void createIndex(String indexName) {
		// TODO Auto-generated method stub
		ESIndexManager.createIndex(indexName);
	}

	@Override
	public void deleteByQuery(SearchConditionPojo searchConditionPojo) {
		ESIndexManager.deleteByQuery(searchConditionPojo);
	}

	@Override
	public void startProcessRootData(String indexName, String indexType,
			String rootPath, Class indexPojoClass) {
		ESIndexManager.startNewIndexProcessTask(indexName, indexType, rootPath,
				indexPojoClass);
	}

	@Override
	public void testSearchData(String indexName, String indexType,
			String rootPath) {
		// TODO Auto-generated method stub
		// ESTestManager.searchBatchByFile(indexName, indexType, rootPath);
	}
}
