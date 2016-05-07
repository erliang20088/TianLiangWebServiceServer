package com.zel.es.utils;

import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.pool.impl.GenericObjectPool.Config;
import org.nutz.ssdb4j.SSDBs;
import org.nutz.ssdb4j.impl.SimpleClient;
import org.nutz.ssdb4j.spi.Response;
import org.nutz.ssdb4j.spi.SSDB;
import org.nutz.ssdb4j.spi.SSDBException;

/**
 * ssdb操作工具类
 * 
 * @author zel
 * 
 */
public class SSDBOperatorUtils {
	private static MyLogger logger = new MyLogger(SSDBOperatorUtils.class);

	private static Lock lock = new ReentrantLock();

	private static SSDB ssdb = null;
	static {
//		reinit();
	}
	public static long last_reinit_time = 0;

	public static void reinit() {
		lock.lock();
		long now_time = System.currentTimeMillis();
		// 本次与上次差距大于1分钟时，则可以重新初始化ssdb连接池
		if ((now_time - last_reinit_time) / 1000 / 60 > 1) {
			try {
				if (ssdb != null) {
					ssdb._depose();
					logger.info("ssdb pool depose successfully~");

					createPool();
					logger.info("ssdb reinit succesfully~");
				} else {
					createPool();
					logger.info("ssdb init succesfully~");
				}
			} catch (Exception e) {
				e.printStackTrace();
				logger.info("ssdb pool depose occur error,will jump this operator!");
			}
		} else {
			logger.info("ssdb pool had reinit ok,not to reinit so frequcy!");
		}
		last_reinit_time = System.currentTimeMillis();
		lock.unlock();
	}

	public static void createPool() {
		Config config = new Config();
		config.maxActive = SystemParas.ssdb_pool_max_active;
		config.maxIdle = SystemParas.ssdb_pool_max_idle;
		ssdb = SSDBs.pool(SystemParas.ssdb_host_ip, SystemParas.ssdb_host_port,
				SystemParas.ssdb_host_timeout, config);
	}

	// 向ssdb server放一个key/value对
	public static void set(String key, String value) {
		try {
			ssdb.set(key, value);
		} catch (Exception e) {
			e.printStackTrace();
			// 发现异常后再尝试放入一次
			ssdb.set(key, value);
		}
	}

	// 向ssdb server放一个key/value对,如果该已经存在，则不做更新存储，直接放弃该次操作
	public static void setnx(String key, String value) {
		try {
			ssdb.setnx(key, value);
		} catch (Exception e) {
			e.printStackTrace();
			// 发现异常后再尝试放入一次
			ssdb.setnx(key, value);
		}
	}

	// 拿到一个key对应的value串
	public static String get(String key) {
		try {
			Response response = ssdb.get(key);
			if (response.ok()) {
				return response.asString();
			}
		} catch (Exception e) {
			e.printStackTrace();
			// 发现异常后再尝试放入一次
			Response response = ssdb.get(key);
			if (response.ok()) {
				return response.asString();
			}
		}
		return null;
	}

	// 基于集合的操作
	public static void pushQueue(String key, String value) {
		try {
			ssdb.qpush(key, value);
		} catch (Exception e) {
			e.printStackTrace();
			// 发现异常后再尝试放入一次
			ssdb.qpush(key, value);
		}
	}

	public static List<String> getQueue(String key) {
		try {
			return ssdb.qslice(key, 0, ssdb.qsize(key).asInt()).listString();
		} catch (Exception e) {
			e.printStackTrace();
			// 发现异常后再尝试放入一次
			return ssdb.qslice(key, 0, ssdb.qsize(key).asInt()).listString();
		}
	}

	// 基于zset的操作
	public static void pushZset(String key, String value, int score) {
		// 遇到time out时，进行重新请求
		int repeat_time = 0;
		int continious_exception_count = 0;
		while (repeat_time < SystemParas.es_index_fail_max_time) {
			try {
				ssdb.zset(key, value, score);
				break;
			} catch (SSDBException timeout) {
				repeat_time++;
				logger.info("ssdb pushZset occur error timeout !");
				timeout.printStackTrace();

				continious_exception_count++;
				if (continious_exception_count == SystemParas.es_client_continious_exception_count_max) {
					reinit();
				}
				try {
					Thread.sleep(SystemParas.es_index_fail_waitting_time);
				} catch (Exception sleepException) {
					sleepException.printStackTrace();
				}
			} catch (Exception e) {
				repeat_time++;
				logger.info("ssdb pushZset occur error,any exception!");
				e.printStackTrace();

				continious_exception_count++;
				if (continious_exception_count == SystemParas.es_client_continious_exception_count_max) {
					reinit();
				}
				try {
					Thread.sleep(SystemParas.es_index_fail_waitting_time);
				} catch (Exception sleepException) {
					sleepException.printStackTrace();
				}
			}
		}
	}

	// 取zset中的key对应的offset位置的len个长度的值
	public static Map<String, String> getZset(String key, int offset, int len) {
		try {
			return ssdb.zrange(key, offset, len).mapString();
		} catch (Exception e) {
			e.printStackTrace();
			// 发现异常后再尝试放入一次
			return ssdb.zrange(key, offset, len).mapString();
		}
	}

	// 得到某个zset的所有值，值中以
	public static Map<String, String> getAllZset(String key) {
		try {
			return ssdb.zrange(key, 0, ssdb.zsize(key).asInt()).mapString();
		} catch (Exception e) {
			e.printStackTrace();
			// 发现异常后再尝试放入一次
			return ssdb.zrange(key, 0, ssdb.zsize(key).asInt()).mapString();
		}
	}

	// 清空指定的key的所有数据，暂不支持直接清空所有数据
	public static void flushAll() {
		try {
			ssdb.flushdb("");
		} catch (Exception e) {
			e.printStackTrace();
			// 发现异常后再尝试放入一次
			ssdb.flushdb("");
		}
	}

	public static void main(String[] args) {
		String key = "123";
		String value = "value_456";

		// SSDBOperatorUtils.flushAll();

		String host = "61.152.73.246";
		int port = 5555;
		int timeout = 5000;
		SSDB ssdb = new SimpleClient(host, port, timeout);

		Response response = ssdb.zlist("", "", 30000);
//		Response response =ssdb.zsize("");
//		ssdb.zsize(arg0)

		System.out.println(response.ok());
		List<String> list = response.listString();
		System.out.println(list.size());
		
		System.out.println("执行完成!");
	}
}
