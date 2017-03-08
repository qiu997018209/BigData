package cn.qiujiahao.bigdata.hdfs;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Before;
import org.junit.Test;
/**
 * 
 * �ͻ���ȥ����hdfsʱ������һ���û���ݵ�
 * Ĭ������£�hdfs�ͻ���api���jvm�л�ȡһ����������Ϊ�Լ����û���ݣ�-DHADOOP_USER_NAME=hadoop
 * 
 * Ҳ�����ڹ���ͻ���fs����ʱ��ͨ���������ݽ�ȥ
 * @author
 *
 */

public class HdfsClientDemo {
	Configuration conf;
	static FileSystem fs;
		
	@Before
	public void init() throws Exception {
		conf = new Configuration();
		//conf.set("fs.defaultFS", "hdfs://192.168.199.3:9000");
		//Ĭ����ʹ�ñ����ļ�ϵͳ���˴�����ʹ��hdfs,�������û����Ϊroot(linux�е��û���)
		//�õ�һ���ļ�ϵͳ�Ŀͻ���ʵ��
		fs = FileSystem.get(new URI("hdfs://192.168.199.3:9000"),conf,"root");    
	}
	
	@Test
	//�鿴conf�ļ�
	public void testConf(){
		Iterator<Entry<String, String>> iterator = conf.iterator();
		while (iterator.hasNext()) {
			Entry<String, String> entry = iterator.next();
			System.out.println(entry.getValue() + "--" + entry.getValue());//conf���ص�����
		}
	}
	@Test
	//�ϴ��ļ�
	public void testUpload() throws Exception {
		fs.copyFromLocalFile(new Path("D:/qiujiahao.txt"), new Path("/qiujiahao.copy1.txt"));
		fs.close();
	}
	//�����ļ�
	@Test
	public void testDownLoad() throws Exception {
		fs.copyToLocalFile(new Path("/qiujiahao.copy.txt"), new Path("D:/qiujiahao4.txt"));
		fs.close();
	}
	//�����ļ���
	@Test
	public void testMkdir() throws Exception {
		boolean mkdirs = fs.mkdirs(new Path("/work2/qiujiahao"));
		fs.close();
		System.out.println(mkdirs);		
	}
	//ɾ���ļ�
	@Test
	public void testDelete() throws Exception {
		boolean delete = fs.delete(new Path("/work2"),true);//true�����Ƿ�ݹ�ɾ��
		fs.close();
		System.out.println(delete);			
	}
	//��ѯָ��Ŀ¼�µ��ļ���Ϣ
	@Test
	public void testls() throws Exception {
		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
		while(listFiles.hasNext()) {
			LocatedFileStatus next = listFiles.next();
			System.out.println("blocksize:"+next.getBlockSize());
			System.out.println("owner:"+next.getOwner());
			System.out.println("name:"+next.getPath().getName());
			System.out.println(".........�ָ���..........");
		}
	}
	

}
