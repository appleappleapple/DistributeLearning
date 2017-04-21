package com.github.distribute.lock;

import java.util.concurrent.TimeUnit;

public interface DistributedLock {

	/**
	 * 尝试获取锁,不进行等待。得到返回true,
	 * 
	 * @return
	 * @throws Exception
	 */
	public boolean tryLock() throws Exception;

	/**
	 * 阻塞等待获取锁
	 * 
	 * @throws Exception
	 */
	public void lock() throws Exception;

	/**
	 * 在规定时间内等待获取锁
	 * 
	 * @param time
	 * @param unit
	 * @return
	 * @throws Exception
	 */
	public boolean lock(long time, TimeUnit unit) throws Exception;

	/**
	 * 释放锁
	 * 
	 * @throws Exception
	 */
	public void unLock() throws Exception;

}
