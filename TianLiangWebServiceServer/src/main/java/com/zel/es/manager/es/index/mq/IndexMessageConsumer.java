package com.zel.es.manager.es.index.mq;

import com.zel.es.manager.es.ESIndexManager;
import com.zel.es.pojos.IndexTaskPojo;
import com.zel.es.utils.MyLogger;
import com.zel.es.utils.SystemParas;

/**
 * 索引消息队列的消费者代程
 * 
 * @author zel
 * 
 */
public class IndexMessageConsumer implements Runnable {
	private static MyLogger logger = new MyLogger(IndexMessageConsumer.class);
	private IndexMessageQueueManager indexMessageQueueManager;
	private ESIndexManager esIndexManager;
	private int cosumerId; 

	public int getCosumerId() {
		return cosumerId;
	}

	public void setCosumerId(int cosumerId) {
		this.cosumerId = cosumerId;
	}

	private IndexTaskPojo indexTaskPojo = null;
	private int waitSecondsNumber;
 
	public IndexMessageConsumer(int cosumerId,
			IndexMessageQueueManager indexMessageQueueManager,
			ESIndexManager esIndexManager) {
		this.cosumerId = cosumerId;
		this.waitSecondsNumber = SystemParas.add_index_sleep_interval_time / 1000;
		this.indexMessageQueueManager = indexMessageQueueManager;
		this.esIndexManager = esIndexManager;
	}

	@Override
	public void run() {
		int no_index_task_continuous_count = 0;
		while (true) {
			indexTaskPojo = indexMessageQueueManager.popOneTaskFromToDoList();
			if (indexTaskPojo == null) {
				try {
					no_index_task_continuous_count++;

					if (no_index_task_continuous_count > 100) {
						// logger.info("idle task count---"+no_index_task_continuous_count);
						// 如果多于100次没有拿到索引任务，则直接退出该消费线程
						logger.info("index consumer thread#" + this.cosumerId
								+ ",idle times count > 100, this consumer thread will exit!");
						break;
					}

					logger.info("index consumer thread#" + this.cosumerId
							+ ",no task to index,will slepp "
							+ this.waitSecondsNumber + " second");
					Thread.sleep(SystemParas.add_index_sleep_interval_time);
				} catch (Exception e) {
					e.printStackTrace();
				}
			} else {
				// 添加索引到es server端
				esIndexManager.addIndexToES(indexTaskPojo.getIndexName(),
						indexTaskPojo.getIndexType(),
						indexTaskPojo.getTodoList());
				// 还原下连续为空的线程计数
				no_index_task_continuous_count = 0;
			}
		}
	}
}
