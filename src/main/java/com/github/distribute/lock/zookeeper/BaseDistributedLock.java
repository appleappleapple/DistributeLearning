package com.github.distribute.lock.zookeeper;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.distribute.lock.DistributedLock;

public abstract class BaseDistributedLock implements DistributedLock {

	private static Logger logger = LoggerFactory.getLogger(BaseDistributedLock.class);

	private ZooKeeper zooKeeper;
	private String rootPath;// 根路径名
	private String lockNamePre;// 锁前缀
	private String currentLockPath;// 用于保存某个客户端在locker下面创建成功的顺序节点，用于后续相关操作使用（如判断）
	private static int MAX_RETRY_COUNT = 10;// 最大重试次数
	

	
	/**
	 * 初始化根目录
	 */
	private void init() {
		try {
			Stat stat = zooKeeper.exists(rootPath, false);// 判断一下根目录是否存在
			if (stat == null) {
				zooKeeper.create(rootPath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}
		} catch (Exception e) {
			logger.error("create rootPath error", e);
		}
	}

	/**
	 * 取得锁的排序号
	 * 
	 * @param str
	 * @param lockName
	 * @return
	 */
	private String getLockNodeNumber(String str, String lockName) {
		int index = str.lastIndexOf(lockName);
		if (index >= 0) {
			index += lockName.length();
			return index <= str.length() ? str.substring(index) : "";
		}
		return str;
	}

	/**
	 * 取得锁的排序列表
	 * 
	 * @return
	 * @throws Exception
	 */
	private List<String> getSortedChildren() throws Exception {
		List<String> children = zooKeeper.getChildren(rootPath, false);
		if (children != null && !children.isEmpty()) {
			Collections.sort(children, new Comparator<String>() {
				public int compare(String lhs, String rhs) {
					return getLockNodeNumber(lhs, lockNamePre).compareTo(getLockNodeNumber(rhs, lockNamePre));
				}
			});
		}
		logger.info("sort childRen:{}", children);
		return children;
	}

	/**
	 * 删除锁节点
	 */
	private void deleteLockNode() {
		try {
			zooKeeper.delete(currentLockPath, -1);
		} catch (Exception e) {
			logger.error("unLock error", e);

		}
	}

	/**
	 * 该方法用于判断自己是否获取到了锁，即自己创建的顺序节点在locker的所有子节点中是否最小.如果没有获取到锁，则等待其它客户端锁的释放，
	 * 并且稍后重试直到获取到锁或者超时
	 * 
	 * @param startMillis
	 * @param millisToWait
	 * @param ourPath
	 * @return
	 * @throws Exception
	 */
	private boolean waitToLock(long startMillis, Long millisToWait) throws Exception {

		boolean haveTheLock = false;
		boolean doDelete = false;

		try {
			while (!haveTheLock) {
				logger.info("get Lock Begin");
				// 该方法实现获取locker节点下的所有顺序节点，并且从小到大排序,
				List<String> children = getSortedChildren();
				String sequenceNodeName = currentLockPath.substring(rootPath.length() + 1);

				// 计算刚才客户端创建的顺序节点在locker的所有子节点中排序位置，如果是排序为0，则表示获取到了锁
				int ourIndex = children.indexOf(sequenceNodeName);

				/*
				 * 如果在getSortedChildren中没有找到之前创建的[临时]顺序节点，这表示可能由于网络闪断而导致
				 * Zookeeper认为连接断开而删除了我们创建的节点，此时需要抛出异常，让上一级去处理
				 * 上一级的做法是捕获该异常，并且执行重试指定的次数 见后面的 attemptLock方法
				 */
				if (ourIndex < 0) {
					logger.error("not find node:{}", sequenceNodeName);
					throw new Exception("节点没有找到: " + sequenceNodeName);
				}

				// 如果当前客户端创建的节点在locker子节点列表中位置大于0，表示其它客户端已经获取了锁
				// 此时当前客户端需要等待其它客户端释放锁，
				boolean isGetTheLock = ourIndex == 0;

				// 如何判断其它客户端是否已经释放了锁？从子节点列表中获取到比自己次小的哪个节点，并对其建立监听
				String pathToWatch = isGetTheLock ? null : children.get(ourIndex - 1);

				if (isGetTheLock) {
					logger.info("get the lock,currentLockPath:{}", currentLockPath);
					haveTheLock = true;
				} else {
					// 如果次小的节点被删除了，则表示当前客户端的节点应该是最小的了，所以使用CountDownLatch来实现等待
					String previousSequencePath = rootPath.concat("/").concat(pathToWatch);
					final CountDownLatch latch = new CountDownLatch(1);
					final Watcher previousListener = new Watcher() {
						public void process(WatchedEvent event) {
							if (event.getType() == EventType.NodeDeleted) {
								latch.countDown();
							}
						}
					};

					// 如果节点不存在会出现异常
					zooKeeper.exists(previousSequencePath, previousListener);

					// 如果有超时时间，刚到超时时间就返回
					if (millisToWait != null) {
						millisToWait -= (System.currentTimeMillis() - startMillis);
						startMillis = System.currentTimeMillis();
						if (millisToWait <= 0) {
							doDelete = true; // timed out - delete our node
							break;
						}

						latch.await(millisToWait, TimeUnit.MICROSECONDS);
					} else {
						latch.await();
					}
				}
			}
		} catch (Exception e) {
			// 发生异常需要删除节点
			logger.error("waitToLock exception", e);
			doDelete = true;
			throw e;
		} finally {
			// 如果需要删除节点
			if (doDelete) {
				deleteLockNode();
			}
		}
		logger.info("get Lock end,haveTheLock=" + haveTheLock);
		return haveTheLock;
	}

	/**
	 * createLockNode用于在locker（basePath持久节点）下创建客户端要获取锁的[临时]顺序节点
	 * 
	 * @param path
	 * @return
	 * @throws Exception
	 */
	private String createLockNode(String path) throws Exception {
		Stat stat = zooKeeper.exists(rootPath, false);
		// 判断一下根目录是否存在
		if (stat == null) {
			zooKeeper.create(rootPath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}
		return zooKeeper.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
	}

	/**
	 * 尝试获取锁，如果不加超时时间，阻塞等待。否则，就是加了超时的阻塞等待
	 * 
	 * @param time
	 * @param unit
	 * @return
	 * @throws Exception
	 */
	protected Boolean attemptLock(long time, TimeUnit unit) throws Exception {
		final long startMillis = System.currentTimeMillis();
		final Long millisToWait = (unit != null) ? unit.toMillis(time) : null;

		boolean hasTheLock = false;
		boolean isDone = false;
		int retryCount = 0;

		// 网络闪断需要重试一试，最大重试次数MAX_RETRY_COUNT
		while (!isDone) {
			isDone = true;
			try {
				currentLockPath = createLockNode(rootPath.concat("/").concat(lockNamePre));
				hasTheLock = waitToLock(startMillis, millisToWait);

			} catch (Exception e) {
				if (retryCount++ < MAX_RETRY_COUNT) {
					isDone = false;
				} else {
					throw e;
				}
			}
		}

		return hasTheLock;
	}
}