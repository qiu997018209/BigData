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

/**����һ����Ŀ�Ļ����ϣ���ͳ�ƽ��������������������
 * @author qiujiahao
 *
 */
public class FlowCountSort  {
	

	
	static class FlowCountMapper extends Mapper<LongWritable, Text, FlowBean, Text> {
		
		//Ϊ����ÿ��һ�д���һ������
		static FlowBean bean = new FlowBean();
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String string = value.toString();
			String[] strings = string.split("\t");
			//ȡ���ֻ���
			String phonenum = strings[0];
			//ȡ�����У���������
			long upflow = Long.parseLong(strings[1]);
			long downflow = Long.parseLong(strings[2]);
			
			//���ö���
			bean.setflow(upflow,downflow);
			//ע��,�˴��Զ���Ϊkey��map��Ĭ�Ͻ��жԴ˶���������򣬵��Ƕ����޷��Ƚ�
			//�����Ҫbean�̳�WritableComparable����дcompareTo����
			//ͬʱ���˴������л�����д���ļ���ʹ�ͳ��д���ڴ��ﲻһ��������ļ����
			//bean����ÿ�ζ��ǲ�һ���ģ������ǵ�ַ
			context.write(bean, new Text(phonenum));
		}
	}
	
	static class FlowCountReducer extends Reducer<FlowBean, Text, Text, FlowBean> {
		
		//��ʱ��ȡ���Ķ����Ѿ����ź�����ˣ�����ֻ��һ������Ϊ������һ����Ŀ�Ļ����ϵ�����ļ�
		//�ĺ�������Ϊmap�����������key�����ź���ģ�������Ҳ��д��compareTo����
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
		
		
		//ָ���������jar�����ڵı���·��
		job.setJarByClass(FlowCountSort.class);
		//job.setJar("/DataTest/FlowCountSort.jar");
		
		//ָ����ҵ��jobҪʹ�õ�mapper/Reducerҵ����
		job.setMapperClass(FlowCountMapper.class);
		job.setReducerClass(FlowCountReducer.class);
		
		//ָ��mapper������ݵ�kv����
		job.setMapOutputKeyClass(FlowBean.class);
		job.setMapOutputValueClass(Text.class);
		
		//ָ��������������ݵ�kv����
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
		
		//ָ��job������ԭʼ�ļ�����Ŀ¼
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		//ָ��job������������Ŀ¼
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//��job�����õ���ز������Լ�job���õ�java�����ڵ�jar�����ύ��yarnȥ����
		/*job.submit();*/
		boolean res = job.waitForCompletion(true);
		System.exit(res?0:1);
	}
}
