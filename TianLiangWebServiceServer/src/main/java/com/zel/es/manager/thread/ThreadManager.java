package com.zel.es.manager.thread;

import java.util.Date;

import com.zel.es.utils.SystemParas;

/**
 * thread manager,主要为控制线程数量等,非严格线程同步
 * 
 * @author zel
 * 
 */
public class ThreadManager {
	// 给同类线程加入线程组
	private ThreadGroup threadGroup = null;

	public ThreadManager(String threadGroupName) {
		threadGroup = new ThreadGroup(threadGroupName);
	}

	public void addRunnableAndStart(Runnable runnable) {
		Thread tt = new Thread(threadGroup, runnable);
		tt.start();
	}

	public ThreadGroup getThreadGroup() {
		return threadGroup;
	}

	public void setThreadGroup(ThreadGroup threadGroup) {
		this.threadGroup = threadGroup;
	}

	public int getActiveCount() {
		return this.threadGroup.activeCount();
	}

	// 得到最多可以再添加多少个线程
	public int getNeedToAddMaxThreadNumber() {
		return SystemParas.add_index_multi_threads_producer_max_number
				- this.getActiveCount();
	}

	public boolean canAddNewThread() {
		if (getNeedToAddMaxThreadNumber() > 0) {
			return true;
		}
		return false;
	}

	static class TestRunnable implements Runnable {
		@Override
		public void run() {
			try {
				Thread.sleep(10 * 1000);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) throws Exception {
		ThreadManager threadManager = new ThreadManager("");
		threadManager.addRunnableAndStart(new TestRunnable());
		threadManager.addRunnableAndStart(new TestRunnable());
		threadManager.addRunnableAndStart(new TestRunnable());
		//
		// Thread.sleep(3000);
		System.out.println(threadManager.getActiveCount());

		System.out.println("执行完成!");
	}
}
