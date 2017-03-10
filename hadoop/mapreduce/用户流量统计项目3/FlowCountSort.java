package cn.qiujiahao.bigdata.mr.flowsum;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import cn.qiujiahao.bigdata.mr.flowsum.FlowCount.FlowCountMapper;
import cn.qiujiahao.bigdata.mr.flowsum.FlowCount.FlowCountReducer;

/**在上一个项目的基础上，将统计结果按照总流量倒序排序
 * @author qiujiahao
 *
 */
public class FlowCountSort  {
	

	
	static class FlowCountMapper extends Mapper<LongWritable, Text, FlowBean, Text> {
		
		//为避免每读一行创建一个对象
		static FlowBean bean = new FlowBean();
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String string = value.toString();
			String[] strings = string.split("\t");
			//取出手机号
			String phonenum = strings[0];
			//取出上行，下行流量
			long upflow = Long.parseLong(strings[1]);
			long downflow = Long.parseLong(strings[2]);
			
			//设置对象
			bean.setflow(upflow,downflow);
			//注意,此处以对象为key，map会默认进行对此对象进行排序，但是对象无法比较
			//因此需要bean继承WritableComparable，重写compareTo方法
			//同时，此处是序列化，会写到文件里，和传统的写到内存里不一样，因此文件里的
			//bean对象每次都是不一样的，并不是地址
			context.write(bean, new Text(phonenum));
		}
	}
	
	static class FlowCountReducer extends Reducer<FlowBean, Text, Text, FlowBean> {
		
		//此时获取到的对象已经是排好序的了，并且只有一个，因为是在上一个项目的基础上的输出文件
		//拍号序是因为map的输出结果里的key都是排好序的，而我们也重写了compareTo方法
		@Override
		protected void reduce(FlowBean bean, Iterable<Text> value, Context context)
				throws IOException, InterruptedException {			
			
			context.write(value.iterator().next(), bean);
		}
		
		
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		/*conf.set("mapreduce.framework.name", "yarn");
		conf.set("yarn.resoucemanager.hostname", "mini1");*/
		Job job = Job.getInstance(conf);
		
		
		//指定本程序的jar包所在的本地路径
		job.setJarByClass(FlowCountSort.class);
		//job.setJar("/DataTest/FlowCountSort.jar");
		
		//指定本业务job要使用的mapper/Reducer业务类
		job.setMapperClass(FlowCountMapper.class);
		job.setReducerClass(FlowCountReducer.class);
		
		//指定mapper输出数据的kv类型
		job.setMapOutputKeyClass(FlowBean.class);
		job.setMapOutputValueClass(Text.class);
		
		//指定最终输出的数据的kv类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
		
		//指定job的输入原始文件所在目录
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		//指定job的输出结果所在目录
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//将job中配置的相关参数，以及job所用的java类所在的jar包，提交给yarn去运行
		/*job.submit();*/
		boolean res = job.waitForCompletion(true);
		System.exit(res?0:1);
	}
}
