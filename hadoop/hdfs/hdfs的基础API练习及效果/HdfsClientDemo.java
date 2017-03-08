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
 * 客户端去操作hdfs时，是有一个用户身份的
 * 默认情况下，hdfs客户端api会从jvm中获取一个参数来作为自己的用户身份：-DHADOOP_USER_NAME=hadoop
 * 
 * 也可以在构造客户端fs对象时，通过参数传递进去
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
		//默认是使用本地文件系统，此处设置使用hdfs,并设置用户身份为root(linux中的用户名)
		//拿到一个文件系统的客户端实例
		fs = FileSystem.get(new URI("hdfs://192.168.199.3:9000"),conf,"root");    
	}
	
	@Test
	//查看conf文件
	public void testConf(){
		Iterator<Entry<String, String>> iterator = conf.iterator();
		while (iterator.hasNext()) {
			Entry<String, String> entry = iterator.next();
			System.out.println(entry.getValue() + "--" + entry.getValue());//conf加载的内容
		}
	}
	@Test
	//上传文件
	public void testUpload() throws Exception {
		fs.copyFromLocalFile(new Path("D:/qiujiahao.txt"), new Path("/qiujiahao.copy1.txt"));
		fs.close();
	}
	//下载文件
	@Test
	public void testDownLoad() throws Exception {
		fs.copyToLocalFile(new Path("/qiujiahao.copy.txt"), new Path("D:/qiujiahao4.txt"));
		fs.close();
	}
	//创建文件夹
	@Test
	public void testMkdir() throws Exception {
		boolean mkdirs = fs.mkdirs(new Path("/work2/qiujiahao"));
		fs.close();
		System.out.println(mkdirs);		
	}
	//删除文件
	@Test
	public void testDelete() throws Exception {
		boolean delete = fs.delete(new Path("/work2"),true);//true代表是否递归删除
		fs.close();
		System.out.println(delete);			
	}
	//查询指定目录下的文件信息
	@Test
	public void testls() throws Exception {
		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
		while(listFiles.hasNext()) {
			LocatedFileStatus next = listFiles.next();
			System.out.println("blocksize:"+next.getBlockSize());
			System.out.println("owner:"+next.getOwner());
			System.out.println("name:"+next.getPath().getName());
			System.out.println(".........分割线..........");
		}
	}
	

}
