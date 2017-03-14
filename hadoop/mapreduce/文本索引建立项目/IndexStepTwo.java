package cn.qiujiahao.bigdata.inverindex;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class IndexStepTwo {

	
	public static class mapmanager extends Mapper<LongWritable, Text, Text, Text> {
		Text temp_word = new Text();
		Text temp_name = new Text();
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] strings = value.toString().split("--");
			//取出单词并以单词为key
			temp_word.set(strings[0]);
			//文件名和此处
			temp_name.set(strings[1]);
			context.write(temp_word, temp_name);
		}
		
	}
	public static class reducemanager extends Reducer<Text, Text, Text, Text> {
	
		@Override
		protected void reduce(Text text, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException {
			StringBuffer buffer = new StringBuffer();
			String string = "";
			for(Text value:values) {
				//将同一个字母出现的所有文件和次数写到一个缓冲区中
				string = value.toString().replace("\t", "-->");
				buffer.append(string+"\t");
			}
			context.write(text, new Text(buffer.toString()));
	}
	}		
	
	public static void main(String[] args) throws Exception {
		
		if (args.length < 1 || args == null) {
			args = new String[]{"D:/test/output/part-r-00000", "D:/test/output4"};
		}
		
		Configuration config = new Configuration();
		Job job = Job.getInstance(config);
		
		job.setMapperClass(mapmanager.class);
		job.setReducerClass(reducemanager.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 1:0);
}
}