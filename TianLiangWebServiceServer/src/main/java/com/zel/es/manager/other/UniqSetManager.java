package com.zel.es.manager.other;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 对某个字符串去重
 * 
 * @author zel
 * 
 */
public class UniqSetManager {
	private Set<String> adSet = new HashSet<String>(1500000);
	private Lock lock = new ReentrantLock();

	public void add(String key) {
		try {
			lock.lock();
			adSet.add(key);
		} finally {
			lock.unlock();
		}
	}

	public boolean contains(String key) {
		// 这里不强调强一致性，故不加锁
		return adSet.contains(key);
	}

}
