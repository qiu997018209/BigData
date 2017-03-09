package cn.qiujiahao.bigdata.proviceflow;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.SnapshottableDirectoryListingProto;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



/**需求：根据归属地输出流量统计数据结果到不同文件，以便于在查询统计结果时可以定位到省级范围进行
 * 
 * mapreduce中，map阶段处理的数据如何传递给reduce阶段，是mapreduce框架中最关键的一个流程，这个流程就叫shuffle；
 * shuffle: 洗牌、发牌——（核心机制：数据分区，排序，缓存）；
 * 具体来说：就是将maptask输出的处理结果数据，分发给reducetask，并在分发的过程中，对数据按key进行了分区和排序；
 * 
 * 本项目实现的机制之一就是重写了shuffle的数据分区方法。
 * @author qiujiahao
 *
 */
public class FlowCount {

	//实现mapper功能
	static class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
		
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException  {
			//将本行数据转为字符串
			String string = value.toString();
			//取出本行数据中的手机号
			String[] split = string.split("\t");
			String phonenum = split[1];
			//取出上行流量和下行流量
			long upFlow = Long.parseLong(split[split.length-3]);
			long downFlow = Long.parseLong(split[split.length-2]);
			//生成自定义的序列化接口
			FlowBean flowBean = new FlowBean(upFlow,downFlow);
			context.write(new Text(phonenum), flowBean);
		}
		
	}
	//实现reducer功能:对每个手机号的流量进行汇总
	//<183323,bean1><183323,bean2><183323,bean3><183323,bean4>.......
	static class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
		
		@Override
		protected void reduce(Text value, Iterable<FlowBean> flowbean, Context context)
				throws IOException, InterruptedException {
				long downflow = 0;
				long upflow = 0;
				long sumflow = 0;
			
				for(FlowBean bean:flowbean) {
					downflow += bean.getDownflow();
					upflow += bean.getUpflow();
				}
				FlowBean flowsum = new FlowBean(upflow,downflow);
				
				context.write(value, flowsum);
		}
		
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		/*conf.set("mapreduce.framework.name", "yarn");
		conf.set("yarn.resoucemanager.hostname", "mini1");*/
		Job job = Job.getInstance(conf);
		
		/*job.setJar("/home/hadoop/wc.jar");*/
		//指定本程序的jar包所在的本地路径
		job.setJarByClass(FlowCount.class);
		
		//指定本业务job要使用的mapper/Reducer业务类
		job.setMapperClass(FlowCountMapper.class);
		job.setReducerClass(FlowCountReducer.class);
		
		//指定mapper输出数据的kv类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBean.class);
		
		//指定我们自定义的数据分区器
		job.setPartitionerClass(ProvincePartitioner.class);
		//指定相应"分区"数量的reduce task
		job.setNumReduceTasks(5);
		
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
