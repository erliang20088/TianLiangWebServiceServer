package com.zel.es.manager.ssdb;

import org.nutz.ssdb4j.impl.SimpleClient;
import org.nutz.ssdb4j.spi.SSDB;

import com.zel.es.manager.ws.client.ServiceClientPool;
import com.zel.es.utils.SystemParas;

/**
 * ssdb 管理器
 * 
 * @author zel
 * 
 */
public class SSDBManager {
	private static ServiceClientPool<SSDB> ssdbClientPool = new ServiceClientPool<SSDB>();

	// 初始化连接池
	static {
		for (int i = 0; i < SystemParas.ssdb_pool_max_active; i++) {
			ssdbClientPool.addServiceClient(createSSDBClient());
		}
	}

	public static SSDB createSSDBClient() {
		SSDB ssdb = new SimpleClient(SystemParas.ssdb_host_ip,
				SystemParas.ssdb_host_port, SystemParas.ssdb_host_timeout);
		return ssdb;
	}
}
