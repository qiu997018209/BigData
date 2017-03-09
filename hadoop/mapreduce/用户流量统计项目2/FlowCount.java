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



/**���󣺸��ݹ������������ͳ�����ݽ������ͬ�ļ����Ա����ڲ�ѯͳ�ƽ��ʱ���Զ�λ��ʡ����Χ����
 * 
 * mapreduce�У�map�׶δ����������δ��ݸ�reduce�׶Σ���mapreduce�������ؼ���һ�����̣�������̾ͽ�shuffle��
 * shuffle: ϴ�ơ����ơ��������Ļ��ƣ����ݷ��������򣬻��棩��
 * ������˵�����ǽ�maptask����Ĵ��������ݣ��ַ���reducetask�����ڷַ��Ĺ����У������ݰ�key�����˷���������
 * 
 * ����Ŀʵ�ֵĻ���֮һ������д��shuffle�����ݷ���������
 * @author qiujiahao
 *
 */
public class FlowCount {

	//ʵ��mapper����
	static class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
		
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException  {
			//����������תΪ�ַ���
			String string = value.toString();
			//ȡ�����������е��ֻ���
			String[] split = string.split("\t");
			String phonenum = split[1];
			//ȡ��������������������
			long upFlow = Long.parseLong(split[split.length-3]);
			long downFlow = Long.parseLong(split[split.length-2]);
			//�����Զ�������л��ӿ�
			FlowBean flowBean = new FlowBean(upFlow,downFlow);
			context.write(new Text(phonenum), flowBean);
		}
		
	}
	//ʵ��reducer����:��ÿ���ֻ��ŵ��������л���
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
		//ָ���������jar�����ڵı���·��
		job.setJarByClass(FlowCount.class);
		
		//ָ����ҵ��jobҪʹ�õ�mapper/Reducerҵ����
		job.setMapperClass(FlowCountMapper.class);
		job.setReducerClass(FlowCountReducer.class);
		
		//ָ��mapper������ݵ�kv����
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBean.class);
		
		//ָ�������Զ�������ݷ�����
		job.setPartitionerClass(ProvincePartitioner.class);
		//ָ����Ӧ"����"������reduce task
		job.setNumReduceTasks(5);
		
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
