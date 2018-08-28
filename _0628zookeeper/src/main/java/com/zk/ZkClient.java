package com.zk;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class ZkClient {
	private String connectString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
	private int sessionTimeout = 2000;
	private ZooKeeper zkClient;

	private void getConnet() throws IOException {

		zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {

			@Override
			public void process(WatchedEvent event) {
				try {
					getServerList();
				} catch (KeeperException | InterruptedException e) {
					e.printStackTrace();
				}
			}
		});
	}

	public static void main(String[] args) throws IOException, KeeperException, InterruptedException {

		ZkClient client = new ZkClient();

		// 1 获取连接
		client.getConnet();

		// 2 监听服务器节点路径
		client.getServerList();

		// 3 业务处理
		client.business();
	}

	private void business() throws InterruptedException {

		Thread.sleep(Long.MAX_VALUE);
	}

	// 获取服务器列表
	private void getServerList() throws KeeperException, InterruptedException {

		List<String> children = zkClient.getChildren("/servers", true);

		// 存储服务器列表
		ArrayList<String> serverList = new ArrayList<>();

		for (String child : children) {
			byte[] data = zkClient.getData("/servers/" + child, false, null);
			serverList.add(new String(data));
		}

		System.out.println(serverList);
	}

}
