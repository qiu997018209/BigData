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
	//��volatile��Ϊ�˱�֤����̵߳�����һ�£������Լ�ά��һ�ݣ�����ÿ�ζ���ȡ
	private static volatile List<String> serverList;
	
	public void ClientConnect() throws Exception {
	    zkCleint = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
			
		@Override
		public void process(WatchedEvent event) {
			// TODO Auto-generated method stub
			try {
				if (null != serverList) {
					//��ʼ����ʱ��Ҳ����һ�������¼�������ʱ��û��server���ߣ���server���ߺ�
					//����sessionTimeout��ʱ���zk�Ż���Ϊ������
					System.out.println("some server is offline!");					
				}

				//����server�ڵ��б�
				GetServerList();
				//����ע������¼�
				zkCleint.getChildren(ParentNode, true);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	});
	}
	
	/**��ȡ��ǰ��server�ڵ�����
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public void  GetServerList() throws KeeperException, InterruptedException {
		//�ȴ���һ���ֲ���list���洢server��Ϣ
		List<String> list = new ArrayList<>();
		List<String> Children = zkCleint.getChildren(ParentNode, true);
		
		for(String child:Children) {
			//childֻ�ǽڵ������
			byte[] child_data = zkCleint.getData(ParentNode+"/"+child, false, null);
			list.add(new String(child_data));
		}
		//���Ÿ�ֵ��serverList
		serverList = list;
		//��ӡ�������б�
		System.out.println(serverList);
	}
	
	/**
	 * ҵ����
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
		//����zk
		client.ClientConnect();
		//����ҵ��
		client.handleBussiness();
	}

}
