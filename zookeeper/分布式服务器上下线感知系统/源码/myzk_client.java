package cn.itdisk.bigdata.zk;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.omg.CORBA.PUBLIC_MEMBER;

public class myzk_client {
	public static final String connectString = "192.168.199.3:2181";
	public static final int sessionTimeout = 30000;
	private static ZooKeeper zkCleint = null;
	private static String ParentNode = "/servers";
	//加volatile是为了保证多个线程的数据一致，不会自己维护一份，而是每次都来取
	private static volatile List<String> serverList;
	
	public void ClientConnect() throws Exception {
	    zkCleint = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
			
		@Override
		public void process(WatchedEvent event) {
			// TODO Auto-generated method stub
			try {
				if (null != serverList) {
					//初始化的时候也会有一个监听事件，但此时并没有server下线，当server下线后
					//会有sessionTimeout的时间后，zk才会认为是下线
					System.out.println("some server is offline!");					
				}

				//更新server节点列表
				GetServerList();
				//继续注册监听事件
				zkCleint.getChildren(ParentNode, true);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	});
	}
	
	/**获取当前的server节点内容
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public void  GetServerList() throws KeeperException, InterruptedException {
		//先创建一个局部的list来存储server信息
		List<String> list = new ArrayList<>();
		List<String> Children = zkCleint.getChildren(ParentNode, true);
		
		for(String child:Children) {
			//child只是节点的名字
			byte[] child_data = zkCleint.getData(ParentNode+"/"+child, false, null);
			list.add(new String(child_data));
		}
		//最后才赋值给serverList
		serverList = list;
		//打印服务器列表
		System.out.println(serverList);
	}
	
	/**
	 * 业务功能
	 * 
	 * @throws InterruptedException
	 */
	public void handleBussiness() throws InterruptedException {
		System.out.println("client start working.....");
		Thread.sleep(Long.MAX_VALUE);
	}
	
	public static void main(String[] args) throws Exception  {
		// TODO Auto-generated method stub
		myzk_client client = new myzk_client();
		//连接zk
		client.ClientConnect();
		//处理业务
		client.handleBussiness();
	}

}
