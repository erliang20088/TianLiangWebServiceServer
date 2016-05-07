package com.zel.test;

import java.util.List;

import org.apache.commons.pool.impl.GenericObjectPool.Config;
import org.nutz.ssdb4j.SSDBs;
import org.nutz.ssdb4j.spi.Response;
import org.nutz.ssdb4j.spi.SSDB;

import com.zel.es.utils.IOUtil;
import com.zel.es.utils.SSDBOperatorUtils;
import com.zel.es.utils.StaticValue;
import com.zel.es.utils.SystemParas;

public class SSDBMainTest {
	public static void ssdbGetOrSet() {
		String ip = "127.0.0.1";
		int port = 9999;
		int timeout = 1000;

		SSDB ssdb = SSDBs.simple(ip, port, timeout);

		ssdb.set("mykey", "myvalue");

		Response response = ssdb.get("mykey");

		System.out.println("ssdb---" + ssdb);
		System.out.println(response.listString());
		System.out.println(response.asString());
	}

	static class SSDBTestRunnable implements Runnable {
		private String filePath;
		private int threadId;

		public SSDBTestRunnable(int threadId, String filePath) {
			this.threadId = threadId;
			this.filePath = filePath;
		}

		@Override
		public void run() {
			int begin_line_number = 0;
			int list_size = 0;
			int total_size_finished = 0;
			String[] colArray = null;
			// 按批量读取出文件中的所有记录
			while (begin_line_number <= SystemParas.add_index_total_max_size) {
				// 读取出指定的文件中的内容
				List<String> txtContent = IOUtil.getLineArrayFromFile(filePath,
						StaticValue.default_encoding, begin_line_number, Math
								.min(SystemParas.add_index_batch_size,
										SystemParas.add_index_total_max_size));

				// 说明已经全部添加完毕了
				if ((list_size = txtContent.size()) <= 0) {
					break;
				}

				for (String line : txtContent) {
					colArray = line.split(StaticValue.separator_tab);
					SSDBOperatorUtils.pushZset("0" + colArray[0] 
							+ this.threadId, "1"+colArray[1]+ "1" + this.threadId, 0);
				}

				begin_line_number = begin_line_number + list_size;
				total_size_finished += list_size;

				System.out.println("thread id#" + this.threadId
						+ ",total_size_finished---" + total_size_finished);
			}
		}

	}

	public static void main(String[] args) {
		String ip = "127.0.0.1";
		int port = 5555;
		int timeout = 1000;

		Config config = new Config();
		config.maxActive = 3;
		// config.
		SSDB ssdb = SSDBs.pool(ip, port, timeout, config);

		String filePath = "d:/tid_output_result.txt";

		for(int j=0;j<10;j++){
			for (int i = 0; i < 60; i++) {
				SSDBTestRunnable ssdbTestRunnable = new SSDBTestRunnable(i,
						filePath);
				new Thread(ssdbTestRunnable).start();
			}
			try{
				Thread.sleep(1000*60*60);	
			}catch(Exception e){
				e.printStackTrace();
			}
		}
		// System.out.println(response.asString());
		System.out.println("执行完成!");
	}
}
