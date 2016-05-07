package com.zel.es.controler;

import com.zel.es.manager.ws.WebServiceManager;
import com.zel.es.utils.MyLogger;

public class SystemControler {
	private static MyLogger logger = new MyLogger(SystemControler.class);

	public static void main(String[] args) {
		/**
		 * 先开始服务
		 */
		WebServiceManager webServiceManager = new WebServiceManager();
		webServiceManager.startWebService();

		// /**
		// * 开启索引线程的消费者类,开启指定个数的消费者线程类
		// */
		// IndexMessageConsumer indexMessageConsumer = null;
		// for (int i = 0; i <
		// SystemParas.add_index_multi_threads_consumer_number; i++) {
		// indexMessageConsumer = new IndexMessageConsumer(i + 1);
		// new Thread(indexMessageConsumer).start();
		// }
	}
}
