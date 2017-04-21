//package com.github.distribute.lock;
//
//import java.util.List;
//import java.util.Set;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.Executors;
//
//import redis.clients.jedis.Jedis;
//import redis.clients.jedis.Transaction;
//
///**
// * redis涔愯閿佸疄渚� 
// * @author linbingwen
// *
// */
//public class OptimisticLockTest {
//
//	public static void main(String[] args) throws InterruptedException {
//		 long starTime=System.currentTimeMillis();
//		
//		 initPrduct();
//		 initClient();
//		 printResult();
//		 
//		long endTime=System.currentTimeMillis();
//		long Time=endTime-starTime;
//		System.out.println("绋嬪簭杩愯鏃堕棿锛� "+Time+"ms");   
//
//	}
//	
//	/**
//	 * 杈撳嚭缁撴灉
//	 */
//	public static void printResult() {
//		Jedis jedis = RedisUtil.getInstance().getJedis();
//		Set<String> set = jedis.smembers("clientList");
//
//		int i = 1;
//		for (String value : set) {
//			System.out.println("绗�" + i++ + "涓姠鍒板晢鍝侊紝"+value + " ");
//		}
//
//		RedisUtil.returnResource(jedis);
//	}
//
//	/*
//	 * 鍒濆鍖栭【瀹㈠紑濮嬫姠鍟嗗搧
//	 */
//	public static void initClient() {
//		ExecutorService cachedThreadPool = Executors.newCachedThreadPool();
//		int clientNum = 10000;// 妯℃嫙瀹㈡埛鏁扮洰
//		for (int i = 0; i < clientNum; i++) {
//			cachedThreadPool.execute(new ClientThread(i));
//		}
//		cachedThreadPool.shutdown();
//		
//		while(true){  
//	            if(cachedThreadPool.isTerminated()){  
//	                System.out.println("鎵�鏈夌殑绾跨▼閮界粨鏉熶簡锛�");  
//	                break;  
//	            }  
//	            try {
//					Thread.sleep(1000);
//				} catch (InterruptedException e) {
//					e.printStackTrace();
//				}    
//	        }  
//	}
//
//	/**
//	 * 鍒濆鍖栧晢鍝佷釜鏁�
//	 */
//	public static void initPrduct() {
//		int prdNum = 100;// 鍟嗗搧涓暟
//		String key = "prdNum";
//		String clientList = "clientList";// 鎶㈣喘鍒板晢鍝佺殑椤惧鍒楄〃
//		Jedis jedis = RedisUtil.getInstance().getJedis();
//
//		if (jedis.exists(key)) {
//			jedis.del(key);
//		}
//		
//		if (jedis.exists(clientList)) {
//			jedis.del(clientList);
//		}
//
//		jedis.set(key, String.valueOf(prdNum));// 鍒濆鍖�
//		RedisUtil.returnResource(jedis);
//	}
//
//}
//
///**
// * 椤惧绾跨▼
// * 
// * @author linbingwen
// *
// */
//class ClientThread implements Runnable {
//	Jedis jedis = null;
//	String key = "prdNum";// 鍟嗗搧涓婚敭
//	String clientList = "clientList";//// 鎶㈣喘鍒板晢鍝佺殑椤惧鍒楄〃涓婚敭
//	String clientName;
//
//	public ClientThread(int num) {
//		clientName = "缂栧彿=" + num;
//	}
//
//	@Override
//	public void run() {
//		try {
//			Thread.sleep((int)(Math.random()*5000));// 闅忔満鐫＄湢涓�涓�
//		} catch (InterruptedException e1) {
//		}
//		while (true) {
//			System.out.println("椤惧:" + clientName + "寮�濮嬫姠鍟嗗搧");
//			jedis = RedisUtil.getInstance().getJedis();
//			try {
//				jedis.watch(key);
//				int prdNum = Integer.parseInt(jedis.get(key));// 褰撳墠鍟嗗搧涓暟
//				if (prdNum > 0) {
//					Transaction transaction = jedis.multi();
//					transaction.set(key, String.valueOf(prdNum - 1));
//					List<Object> result = transaction.exec();
//					if (result == null || result.isEmpty()) {
//						System.out.println("鎮插墽浜嗭紝椤惧:" + clientName + "娌℃湁鎶㈠埌鍟嗗搧");// 鍙兘鏄痺atch-key琚閮ㄤ慨鏀癸紝鎴栬�呮槸鏁版嵁鎿嶄綔琚┏鍥�
//					} else {
//						jedis.sadd(clientList, clientName);// 鎶㈠埌鍟嗗搧璁板綍涓�涓�
//						System.out.println("濂介珮鍏达紝椤惧:" + clientName + "鎶㈠埌鍟嗗搧");
//						break;
//					}
//				} else {
//					System.out.println("鎮插墽浜嗭紝搴撳瓨涓�0锛岄【瀹�:" + clientName + "娌℃湁鎶㈠埌鍟嗗搧");
//					break;
//				}
//			} catch (Exception e) {
//				e.printStackTrace();
//			} finally {
//				jedis.unwatch();
//				RedisUtil.returnResource(jedis);
//			}
//
//		}
//	}
//
//}
