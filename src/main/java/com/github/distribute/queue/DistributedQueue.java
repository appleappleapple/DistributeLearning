package com.github.distribute.queue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.I0Itec.zkclient.ExceptionUtil;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 分布式队列，同步队列的实现
 * 
 * @author linbingwen
 *
 * @param <T>
 */
public class DistributedQueue<T> {
	private static Logger logger = LoggerFactory.getLogger(DistributedQueue.class);

	protected final ZooKeeper zooKeeper;// 用于操作zookeeper集群
	protected final String root;// 代表根节点
	private int queueSize;
	private String startPath = "/queue/start";

	protected static final String Node_NAME = "n_";// 顺序节点的名称

	public DistributedQueue(ZooKeeper zooKeeper, String root, int queueSize) {
		this.zooKeeper = zooKeeper;
		this.root = root;
		this.queueSize = queueSize;
		init();
	}

	/**
	 * 初始化根目录
	 */
	private void init() {
		try {
			Stat stat = zooKeeper.exists(root, false);// 判断一下根目录是否存在
			if (stat == null) {
				zooKeeper.create(root, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}
			zooKeeper.delete(startPath, -1); // 删除队列满的标志
		} catch (Exception e) {
			logger.error("create rootPath error", e);
		}
	}

	/**
	 * 获取队列的大小
	 * 
	 * @return
	 * @throws Exception
	 */
	public int size() throws Exception {
		return zooKeeper.getChildren(root, false).size();
	}

	/**
	 * 判断队列是否为空
	 * 
	 * @return
	 * @throws Exception
	 */
	public boolean isEmpty() throws Exception {
		return zooKeeper.getChildren(root, false).size() == 0;
	}

	/**
	 * bytes 转object
	 * 
	 * @param bytes
	 * @return
	 */
	private Object ByteToObject(byte[] bytes) {
		Object obj = null;
		try {
			// bytearray to object
			ByteArrayInputStream bi = new ByteArrayInputStream(bytes);
			ObjectInputStream oi = new ObjectInputStream(bi);

			obj = oi.readObject();
			bi.close();
			oi.close();
		} catch (Exception e) {
			logger.error("translation" + e.getMessage());
			e.printStackTrace();
		}
		return obj;
	}

	/**
	 * Object 转byte
	 * 
	 * @param obj
	 * @return
	 */
	private byte[] ObjectToByte(java.lang.Object obj) {
		byte[] bytes = null;
		try {
			// object to bytearray
			ByteArrayOutputStream bo = new ByteArrayOutputStream();
			ObjectOutputStream oo = new ObjectOutputStream(bo);
			oo.writeObject(obj);

			bytes = bo.toByteArray();

			bo.close();
			oo.close();
		} catch (Exception e) {
			logger.error("translation" + e.getMessage());
			e.printStackTrace();
		}
		return bytes;
	}

	/**
	 * 向队列提供数据,队列满的话会阻塞等待直到start标志位清除
	 * 
	 * @param element
	 * @return
	 * @throws Exception
	 */
	public boolean offer(T element) throws Exception {
		// 构建数据节点的完整路径
		String nodeFullPath = root.concat("/").concat(Node_NAME);
		try {
			if (queueSize > size()) {
				// 创建持久的节点，写入数据
				zooKeeper.create(nodeFullPath, ObjectToByte(element), ZooDefs.Ids.OPEN_ACL_UNSAFE,
						CreateMode.PERSISTENT);
				// 再判断一下队列是否满
				if (queueSize > size()) {
					zooKeeper.delete(startPath, -1); // 确保不存在
				} else {
					zooKeeper.create(startPath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				}
			} else {
				// 创建队列满的标记
				if (zooKeeper.exists(startPath, false) != null) {
					zooKeeper.create(startPath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				}

				final CountDownLatch latch = new CountDownLatch(1);
				final Watcher previousListener = new Watcher() {
					public void process(WatchedEvent event) {
						if (event.getType() == EventType.NodeDeleted) {
							latch.countDown();
						}
					}
				};

				// 如果节点不存在会出现异常
				zooKeeper.exists(startPath, previousListener);
				latch.await();
				offer(element);

			}
		} catch (ZkNoNodeException e) {
			logger.error("", e);
		} catch (Exception e) {
			throw ExceptionUtil.convertToRuntimeException(e);
		}
		return true;
	}

	/**
	 * 从队列取数据,当有start标志位时，开始取数据，全部取完数据后才删除start标志
	 * 
	 * @return
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	public T poll() throws Exception {

		try {
			// 队列还没满
			if (zooKeeper.exists(startPath, false) == null) {
				final CountDownLatch latch = new CountDownLatch(1);
				final Watcher previousListener = new Watcher() {
					public void process(WatchedEvent event) {
						if (event.getType() == EventType.NodeCreated) {
							latch.countDown();
						}
					}
				};

				// 如果节点不存在会出现异常
				zooKeeper.exists(startPath, previousListener);

				// 如果节点不存在会出现异常
				latch.await();
			}

			List<String> list = zooKeeper.getChildren(root, false);
			if (list.size() == 0) {
				return null;
			}
			// 将队列按照由小到大的顺序排序
			Collections.sort(list, new Comparator<String>() {
				public int compare(String lhs, String rhs) {
					return getNodeNumber(lhs, Node_NAME).compareTo(getNodeNumber(rhs, Node_NAME));
				}
			});

			/**
			 * 将队列中的元素做循环，然后构建完整的路径，在通过这个路径去读取数据
			 */
			for (String nodeName : list) {
				String nodeFullPath = root.concat("/").concat(nodeName);
				try {
					T node = (T) ByteToObject(zooKeeper.getData(nodeFullPath, false, null));
					zooKeeper.delete(nodeFullPath, -1);
					return node;
				} catch (ZkNoNodeException e) {
					logger.error("", e);
				}
			}
			return null;
		} catch (Exception e) {
			throw ExceptionUtil.convertToRuntimeException(e);
		}

	}

	/**
	 * 截取节点的数字的方法
	 * 
	 * @param str
	 * @param nodeName
	 * @return
	 */
	private String getNodeNumber(String str, String nodeName) {
		int index = str.lastIndexOf(nodeName);
		if (index >= 0) {
			index += Node_NAME.length();
			return index <= str.length() ? str.substring(index) : "";
		}
		return str;

	}

}
