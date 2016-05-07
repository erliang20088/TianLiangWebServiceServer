package com.zel.es.manager.es.index.mq;

import java.util.LinkedList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.zel.es.pojos.IndexTaskPojo;
import com.zel.es.utils.MyLogger;
import com.zel.es.utils.SystemParas;

/**
 * 索引数据的消息队列，这是典型的生产者-消费者模式，该地方充当中间的资源池,主要是负责同步等
 *  
 * @author zel
 */
public class IndexMessageQueueManager {
	private static MyLogger logger = new MyLogger(
			IndexMessageQueueManager.class);
	// 同步锁，替代synchronzied
	private Lock lock = new ReentrantLock();

	// 存放链接池的集合类
	private LinkedList<IndexTaskPojo> todoIndexList = new LinkedList<IndexTaskPojo>();

	public IndexMessageQueueManager() {
	}

	// 将一个任放入任务对列
	public void pushToDoList(IndexTaskPojo indexTaskPojo) {
		lock.lock();
		try {
			todoIndexList.push(indexTaskPojo);
		} finally {
			lock.unlock();
		}
	}

	// 取出任务队列中的一个任务
	public IndexTaskPojo popOneTaskFromToDoList() {
		IndexTaskPojo t = null;
		lock.lock();
		try {
			t = todoIndexList.pollFirst();
		} catch (Exception e) {
			// 如果为空，则代表队列已空,直接返回null即可
			return null;
		} finally {
			lock.unlock();
		}
		return t;
	}

	// 求资源池的长度，如果为空，则要执消费线程等待/sleep
	// 因在本应用中该值不需求严格与实际相同，故不加锁，以提高相关的效率
	public int todoListSize() {
		return todoIndexList.size();
	}

	public boolean isShouldWait() {
		if (todoListSize() > SystemParas.add_index_multi_threads_resource_number_threshold) {
			return true;
		} else {
			return false;
		}
	}
}
