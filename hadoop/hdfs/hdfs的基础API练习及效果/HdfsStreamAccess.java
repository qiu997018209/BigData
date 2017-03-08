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

/**ͨ�����ķ�ʽ����hadoop
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
		//Ĭ����ʹ�ñ����ļ�ϵͳ���˴�����ʹ��hdfs,�������û����Ϊroot(linux�е��û���)
		//�õ�һ���ļ�ϵͳ�Ŀͻ���ʵ��
		fs = FileSystem.get(new URI("hdfs://192.168.199.3:9000"),conf,"root");    
	}
	

	//�ϴ��ļ�
	@Test
	public void testUploadBysream() throws Exception {
		FSDataOutputStream outputStream = fs.create(new Path("/qiujiahao5.txt"),true);
		FileInputStream inputStream = new FileInputStream("D:/qiujiahao5.txt");
		IOUtils.copy(inputStream, outputStream);
	}
	//�����ļ�
	@Test
	public void testDownloadBysream() throws Exception {
		FSDataInputStream inputStream = fs.open(new Path("/qiujiahao5.txt"));
		FileOutputStream outputStream = new FileOutputStream("D:/qiujiahao6.txt");
		IOUtils.copy(inputStream, outputStream);
		
	}
	//ָ���ļ���ʼ��ַ����ȡ�ļ�����
	public void testRandomReadBysream() throws Exception {
		FSDataInputStream inputStream = fs.open(new Path("/qiujiahao5.txt"));
		//ָ����ʼλ��
		inputStream.seek(2);
		FileOutputStream outputStream = new FileOutputStream("D:/qiujiahao6.txt");
		IOUtils.copy(inputStream, outputStream);
	}
}
