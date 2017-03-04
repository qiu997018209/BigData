package cn.itdisk.bigdata.zk;

import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

/**
 * @author qiujiahao
 *
 */
/**
 * @author qiujiahao
 *
 */
public class myzk_server {
	public static final String connectString = "192.168.199.3:2181";
	public static final int sessionTimeout = 30000;
	private static ZooKeeper zkCleint = null;
	private static String ParentNode = "/servers";
	
	/**
	 * 连接服务器，实际就是创建一个zk
	 * @throws Exception
	 */
	public void ServerConnect() throws Exception {
	    zkCleint = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
			
		@Override
		public void process(WatchedEvent event) {
			// TODO Auto-generated method stub
			try {
				System.out.println("current server is online");
				//初次建立连接的时候，会产生一个初始化的事件
				//zkCleint.getChildren(ParentNode, true);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	});
	}
	
	/**
	 * 创建server临时且序列化的节点，这样服务器连接断开节点就会消失，并且是序列化的，不会冲突
	 * @throws Exception 
	 * @throws KeeperException 
	 */
	public void RegisterServer(String subNode) throws Exception {
		zkCleint.create(ParentNode+"/"+subNode, subNode.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		System.out.println(ParentNode+"/"+subNode+":创建成功");
	}
	/**
	 * 业务处理
	 * @param subNode
	 * @throws Exception
	 */
	public void HandleBusines (String subNode) throws Exception {
		System.out.println(subNode+"开始工作");
		//线程不结束，持续等待监听事件
		Thread.sleep(Long.MAX_VALUE);
	}
	
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		myzk_server my_server = new myzk_server();
		//连接zk
		my_server.ServerConnect();
		//建立zk节点
		my_server.RegisterServer(args[0]);
		//处理业务
		my_server.HandleBusines(args[0]);
		
	}
}
