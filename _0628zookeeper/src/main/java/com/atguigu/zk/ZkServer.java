package com.atguigu.zk;

import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

public class ZkServer {

	private String connectString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
	// 获取连接
	private int sessionTimeout = 2000;
	private ZooKeeper zkClient;
	private String pathNode = "/servers";

	public void getConnect() throws IOException {

		zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {

			@Override
			public void process(WatchedEvent event) {

			}
		});
	}

	public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
		args = new String[] { "hadoop103"};

		ZkServer server = new ZkServer();

		// 1 获取连接zkServer
		server.getConnect();

		// 2 注册服务器节点信息
		server.regist(args[0]);

		// 3 业务逻辑处理
		server.business(args[0]);
	}

	private void business(String hostnam) throws InterruptedException {
		System.out.println(hostnam + "is online!");
		Thread.sleep(Long.MAX_VALUE);
	}

	// 注册服务器节点
	private void regist(String hostname) throws KeeperException, InterruptedException {

		String path = zkClient.create(pathNode + "/server", hostname.getBytes(), Ids.OPEN_ACL_UNSAFE,
				CreateMode.EPHEMERAL_SEQUENTIAL);

		System.out.println(path);
	}
}
