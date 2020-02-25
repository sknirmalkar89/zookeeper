package com.election;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event;
import org.apache.zookeeper.data.Stat;

public class Leader implements Watcher{

	public static final String ZOOKEEPER_ADDRESS = "localhost:2181";
	public static final int SESSION_TIMEOUT = 10000;
	public static final String ELECTION_NAMESPACE = "/election";
	public String currentZnode;
	
	private ZooKeeper zk;
	public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
		Leader leader = new Leader();
		leader.connectZookeeper();
		leader.volunteerForLeadership();
		leader.reelectLeader();
		leader.run();
		leader.close();
	}

	void connectZookeeper() throws IOException {
		this.zk = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);
	}
	
	public void volunteerForLeadership() throws KeeperException, InterruptedException {
		String znodePrefix = ELECTION_NAMESPACE + "/c_";
		String znodeFullPath;

		Stat s = zk.exists(ELECTION_NAMESPACE, false);
		if (s == null) {
			zk.create(ELECTION_NAMESPACE, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

		}
		znodeFullPath = zk.create(znodePrefix, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
				CreateMode.EPHEMERAL_SEQUENTIAL);
		System.out.println(znodeFullPath);
		this.currentZnode = znodeFullPath.replace(ELECTION_NAMESPACE + "/", "");

	}
	
	
	public void reelectLeader() throws KeeperException, InterruptedException {
		Stat predCStat = null;
		String predCNode = "";

		while (predCStat == null) {
			List<String> children = zk.getChildren(ELECTION_NAMESPACE, false);

			Collections.sort(children);

			String smallest = children.get(0);
			if (this.currentZnode.equals(smallest)) {
				System.out.println("I am the leader");
				return;
			} else {

				System.out.println("I am not the Leader");
				int predC = Collections.binarySearch(children, currentZnode) - 1;
				predCNode = children.get(predC);
				predCStat = zk.exists(ELECTION_NAMESPACE + "/" + predCNode, this);
			}
		}

		System.out.println("watching : " + predCNode);

	}
	

	@Override
	public void process(WatchedEvent event) {
		switch (event.getType()) {
		case None:
			if (event.getState() == Event.KeeperState.SyncConnected) {
				System.out.println("Connected to Zookeeper server");
			} else {
				synchronized (zk) {
					System.out.println("Disconnected from zookeeper");
					zk.notifyAll();
				}
			}
			break;

		case NodeDeleted:
			try {
				reelectLeader();
			} catch (KeeperException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			break;
		}
	}
	
	private void run() throws InterruptedException {
		synchronized (zk) {
			zk.wait();
		}
	}

	public void close() throws InterruptedException {
		zk.close();
	}
	
}
