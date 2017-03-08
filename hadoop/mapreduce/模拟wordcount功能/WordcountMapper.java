package cn.qiujiahao.bigdata.mr.wcdemo;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * KEYIN: Ĭ������£���mr�����������һ���ı�����ʼƫ������Long,
 * ������hadoop�����Լ��ĸ���������л��ӿڣ����Բ�ֱ����Long������LongWritable
 * 
 * VALUEIN:Ĭ������£���mr�����������һ���ı������ݣ�String��ͬ�ϣ���Text
 * 
 * KEYOUT�����û��Զ����߼��������֮����������е�key���ڴ˴��ǵ��ʣ�String��ͬ�ϣ���Text
 * VALUEOUT�����û��Զ����߼��������֮����������е�value���ڴ˴��ǵ��ʴ�����Integer��ͬ�ϣ���IntWritable
 * 
 * @author
 *
 */
public class WordcountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		//��mapreduce�������ǵ�������ת���ַ���
		String string = value.toString();
		//���ݿո���һ���зֳɵ���
		String[] split = string.split(" ");
		//���������Ϊ<���ʣ�1>
		for (String word:split) {
			//��������Ϊkey��value��Ϊ1���Ա��ں��������ݷַ���
			//���Ը��ݵ��ʷַ����Ա�����ͬ�ĵ��ʻ�ָ���ͬ��reduce task
			context.write(new Text(word), new IntWritable(1));
		}
	}
}
