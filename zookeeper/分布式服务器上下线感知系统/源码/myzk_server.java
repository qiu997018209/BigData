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
	 * ���ӷ�������ʵ�ʾ��Ǵ���һ��zk
	 * @throws Exception
	 */
	public void ServerConnect() throws Exception {
	    zkCleint = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
			
		@Override
		public void process(WatchedEvent event) {
			// TODO Auto-generated method stub
			try {
				System.out.println("current server is online");
				//���ν������ӵ�ʱ�򣬻����һ����ʼ�����¼�
				//zkCleint.getChildren(ParentNode, true);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	});
	}
	
	/**
	 * ����server��ʱ�����л��Ľڵ㣬�������������ӶϿ��ڵ�ͻ���ʧ�����������л��ģ������ͻ
	 * @throws Exception 
	 * @throws KeeperException 
	 */
	public void RegisterServer(String subNode) throws Exception {
		zkCleint.create(ParentNode+"/"+subNode, subNode.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		System.out.println(ParentNode+"/"+subNode+":�����ɹ�");
	}
	/**
	 * ҵ����
	 * @param subNode
	 * @throws Exception
	 */
	public void HandleBusines (String subNode) throws Exception {
		System.out.println(subNode+"��ʼ����");
		//�̲߳������������ȴ������¼�
		Thread.sleep(Long.MAX_VALUE);
	}
	
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		myzk_server my_server = new myzk_server();
		//����zk
		my_server.ServerConnect();
		//����zk�ڵ�
		my_server.RegisterServer(args[0]);
		//����ҵ��
		my_server.HandleBusines(args[0]);
		
	}
}
