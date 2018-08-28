package com.atguigu.zk;

import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

public class TestZookeeper {

	// 连接zkServer
	private String connectString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
	// 超时时间设置
	private int sessionTimeout = 2000;
	ZooKeeper zkClient;

	// 初始化zk客户端
	@Before
	public void initClient() throws IOException {

		zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {

			@Override
			public void process(WatchedEvent event) {
				// System.out.println(event.getType()+"\t"+event.getPath());
				
				System.out.println("----------start-----------");
				// 又调一次监听
//				List<String> children;
//				try {
//					children = zkClient.getChildren("/", true);// 又调一次监听
//
//					for (String child : children) {
//
//						System.out.println("Watcher(): " + child);
//					}
					
				try {
					Stat stat = zkClient.exists("/atguigu", true);
					System.out.println(stat == null ? "not exist" : "exist");
					
					System.out.println("----------end-----------");
				} catch (KeeperException | InterruptedException e) {
					e.printStackTrace();
				}

			}
		});
	}

	// 创建子节点
	@Test
	public void create() throws KeeperException, InterruptedException {
		String path = zkClient.create("/atguigu", "jinlian".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

		System.out.println(path);
	}

	// 获取子节点
	@Test
	public void getChild() throws KeeperException, InterruptedException {

		List<String> children = zkClient.getChildren("/", true);

		for (String child : children) {

			System.out.println("getChild(): " + child);

		}

		Thread.sleep(Long.MAX_VALUE);
	}

	// 判断节点是否存在
	@Test
	public void exist() throws KeeperException, InterruptedException {

		Stat stat = zkClient.exists("/atguigu", true);//是否监听

		System.out.println(stat == null ? "not exist" : "exist");
		
		Thread.sleep(Long.MAX_VALUE);
	}
}
