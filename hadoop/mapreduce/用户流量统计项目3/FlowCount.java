package cn.qiujiahao.bigdata.mr.flowsum;

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



/**本项目的意义在于学习在自定义对象实现MR中的序列化接口
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
		//conf.set("fs.defaultFS", "hdfs://192.168.199.3:9000");
		//conf.set("mapreduce.framework.name", "yarn");
		//conf.set("yarn.resoucemanager.hostname", "192.168.199.3");
		Job job = Job.getInstance(conf);
		
		
		//指定本程序的jar包所在的本地路径
		job.setJarByClass(FlowCount.class);
		//job.setJar("/DataTest/FlowCount.jar");
		
		//指定本业务job要使用的mapper/Reducer业务类
		job.setMapperClass(FlowCountMapper.class);
		job.setReducerClass(FlowCountReducer.class);
		
		//指定mapper输出数据的kv类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBean.class);
		
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
