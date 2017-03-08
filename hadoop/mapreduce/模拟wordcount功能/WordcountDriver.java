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
 * �൱��һ��yarn��Ⱥ�Ŀͻ���
 * ��Ҫ�ڴ˷�װ���ǵ�mr�����������в�����ָ��jar��
 * ����ύ��yarn
 * @author
 *
 */
public class WordcountDriver {

		public static void main(String[] args) throws Exception {
			
			Configuration conf = new Configuration();
			//���ڱ���������linux�ϵ�hadoop��Ⱥ��ִ�У����Բ����趨resourcemanager�ȵ�·��
			Job job = Job.getInstance(conf);
			
			//ָ�����ļ�jar�����ڵ�·��:���Ǳ��ļ����ڵ�·��
			job.setJarByClass(WordcountDriver.class);
			
			//ָ����ҵ�����õ���mapper��reducer
			job.setMapperClass(WordcountMapper.class);
			job.setReducerClass(WordcountReducer.class);
			
			//ָ��mapper��kv�������
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			
			//ָ�����������kv�������
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			
			//ָ��job�������ļ�����Ŀ¼
			FileInputFormat.setInputPaths(job, new Path(args[0]));
			
			//ָ��job������ļ�����Ŀ¼
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			
			//��job�����õ���ز������Լ�job���õ�java�����ڵ�jar�����ύ��yarnȥ����
			//��job.submit()���һ�����ܾ��ǵȴ�yarn�����Ľ��
			boolean completion = job.waitForCompletion(true);
			System.exit(completion?1:0);
		}
}
