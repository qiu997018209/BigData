package cn.qiujiahao.bigdata.hdfs;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.URI;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

/**通过流的方式访问hadoop
 * @author qiujiahao
 *
 */
public class HdfsStreamAccess {
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
	

	//上传文件
	@Test
	public void testUploadBysream() throws Exception {
		FSDataOutputStream outputStream = fs.create(new Path("/qiujiahao5.txt"),true);
		FileInputStream inputStream = new FileInputStream("D:/qiujiahao5.txt");
		IOUtils.copy(inputStream, outputStream);
	}
	//下载文件
	@Test
	public void testDownloadBysream() throws Exception {
		FSDataInputStream inputStream = fs.open(new Path("/qiujiahao5.txt"));
		FileOutputStream outputStream = new FileOutputStream("D:/qiujiahao6.txt");
		IOUtils.copy(inputStream, outputStream);
		
	}
	//指定文件起始地址，获取文件内容
	public void testRandomReadBysream() throws Exception {
		FSDataInputStream inputStream = fs.open(new Path("/qiujiahao5.txt"));
		//指定开始位置
		inputStream.seek(2);
		FileOutputStream outputStream = new FileOutputStream("D:/qiujiahao6.txt");
		IOUtils.copy(inputStream, outputStream);
	}
}
