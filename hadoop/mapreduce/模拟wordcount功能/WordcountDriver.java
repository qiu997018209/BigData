package cn.qiujiahao.bigdata.mr.wcdemo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * 相当于一个yarn集群的客户端
 * 需要在此封装我们的mr程序的相关运行参数，指定jar包
 * 最后提交给yarn
 * @author
 *
 */
public class WordcountDriver {

		public static void main(String[] args) throws Exception {
			
			Configuration conf = new Configuration();
			//由于本程序是在linux上的hadoop集群里执行，所以不用设定resourcemanager等的路径
			Job job = Job.getInstance(conf);
			
			//指定本文件jar包所在的路径:就是本文件所在的路径
			job.setJarByClass(WordcountDriver.class);
			
			//指定本业务所用到的mapper和reducer
			job.setMapperClass(WordcountMapper.class);
			job.setReducerClass(WordcountReducer.class);
			
			//指定mapper的kv输出类型
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			
			//指定最终输出的kv输出类型
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			
			//指定job的输入文件所在目录
			FileInputFormat.setInputPaths(job, new Path(args[0]));
			
			//指定job的输出文件所在目录
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			
			//将job中配置的相关参数，以及job所用的java类所在的jar包，提交给yarn去运行
			//比job.submit()多的一个功能就是等待yarn反馈的结果
			boolean completion = job.waitForCompletion(true);
			System.exit(completion?1:0);
		}
}
