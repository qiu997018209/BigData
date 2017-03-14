package cn.qiujiahao.bigdata.mjoin;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.mortbay.util.StringUtil;

import com.sun.org.apache.bcel.internal.generic.NEW;

import javafx.scene.shape.Line;

/**
 * 本项目是在前一个项目:reduce端实现用户订单合并的基础上实现的
 * 需求：在前一个项目的基础上解决数据顷斜的问题，即：
 * 		 分区的时候是按照hashcode%reduce task,但是实际情况下，很可能某种订单的数量很多
 * 		 这会导致某一个reduce task上需要出来的数据非常多，存在数据顷斜的可能
 * 
 * @author qiujiahao
 *
 */
public class mjoin {

	
	public static class MapJoinManager extends  Mapper<LongWritable, Text, Text, NullWritable> {
		
		//阅读源码发现，在构建Mapper前会调用setup方法,利用它来实现产品信息的缓存，这样每个map中都
		//存在一份产品文件信息
		HashMap<String, String> product_info = new HashMap<String, String>();
		
		
		@Override
		protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			//super.setup(context);
			//从缓存中读取文件,该文件在main里提前读到缓存中
			BufferedReader pro_reader = new BufferedReader(new InputStreamReader(new FileInputStream("pdts.txt")));
			//提取产品信息
			String Line;
			while(StringUtils.isNotEmpty(Line = pro_reader.readLine())) {
				String[] split = Line.split(",");
				//产品编号：产品名字
				product_info.put(split[0], split[1]);			
			}
			pro_reader.close();
					
		}
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String string = value.toString();
			String[] strings = string.split("\t");
			//获取产品编号
			String product_num = strings[2];
			//根据产品编号取hashmap里取这个产品的名字
			String product_name = product_info.get(product_num);
			Text my_text = new Text();
			//在本行中新添加产品名字信息
			my_text.set(string+"\t"+product_name);
			context.write(my_text, NullWritable.get());			
		}
	}
	
	
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Path path = new Path("file:/D:/test/mapjoincache/pdts.txt");
		DistributedCache.addCacheFile(path.toUri(),conf);//放在new Job之前，否则读不到CacheFile
		//conf.set("mapred.cache.files", "D:/test/mapjoininput");
		Job job = Job.getInstance(conf);

		//job.setJarByClass(MapJoinManager.class);

		job.setMapperClass(MapJoinManager.class);
		//只需要指定最终的输出
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.setInputPaths(job, new Path("D:/test/mapjoininput"));
		FileOutputFormat.setOutputPath(job, new Path("D:/test/output5"));

		// 指定需要缓存一个文件到所有的maptask运行节点工作目录
		/* job.addArchiveToClassPath(archive); */// 缓存jar包到task运行节点的classpath中
		/* job.addFileToClassPath(file); */// 缓存普通文件到task运行节点的classpath中
		/* job.addCacheArchive(uri); */// 缓存压缩包文件到task运行节点的工作目录
		/* job.addCacheFile(uri) */// 缓存普通文件到task运行节点的工作目录

		// 将产品表文件缓存到task工作节点的工作目录中去
		job.addCacheFile(new URI("file:/D:/test/mapjoincache/pdts.txt"));
		
		//map端join的逻辑不需要reduce阶段，设置reducetask数量为0
		job.setNumReduceTasks(0);
		boolean res = job.waitForCompletion(true);
		System.exit(res ? 0 : 1);

	}
	
	
}
