package cn.qiujiahao.bigdata.inverindex;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import cn.qiujiahao.bigdata.mjoin.mjoin.MapJoinManager;

/**
 * 需求：有大量的文本（文档、网页），需要建立搜索索引
 * @author qiujiahao
 *
 */
public class IndexStepOne {
	
	public static class MapJoinManager extends Mapper<LongWritable, Text, Text, IntWritable> {
		 Text k = new Text();
		 IntWritable v = new IntWritable(1);
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			 String[] words = value.toString().split(" ");
			 FileSplit file = (FileSplit)context.getInputSplit();
			 String filename = file.getPath().getName();
			 for(String word:words) {
				 //将本行出现的每一个单词与文件名组装成key
				 k.set(word+"--"+filename);
				 context.write(k, v);
			 }
		}
		
	}
	
	public static class ReduceManager extends Reducer<Text, IntWritable, Text, IntWritable> {
		
		@Override
		protected void reduce(Text text, Iterable<IntWritable> value,Context contex) throws IOException, InterruptedException {
			//此处统计每个单词在同一个文件出现的总次数
			int count = 0;
			for (IntWritable num:value) {
				count += num.get();
			}
			contex.write(text,new IntWritable(count));;
		}
		
		
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		job.setJarByClass(IndexStepOne.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.setInputPaths(job, new Path("D:/test/input"));
		FileOutputFormat.setOutputPath(job, new Path("D:/test/output"));
		// FileInputFormat.setInputPaths(job, new Path(args[0]));
		// FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(MapJoinManager.class);
		job.setReducerClass(ReduceManager.class);

		job.waitForCompletion(true);
	}
	
}
