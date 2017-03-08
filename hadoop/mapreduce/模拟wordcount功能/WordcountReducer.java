package cn.qiujiahao.bigdata.mr.wcdemo;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * KEYIN, VALUEIN ��Ӧ  mapper�����KEYOUT,VALUEOUT���Ͷ�Ӧ
 * 
 * KEYOUT, VALUEOUT ���Զ���reduce�߼��������������������
 * KEYOUT�ǵ���
 * VLAUEOUT���ܴ���
 * @author
 *
 */
public class WordcountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	/**
	 * <angelababy,1><angelababy,1><angelababy,1><angelababy,1><angelababy,1>
	 * <hello,1><hello,1><hello,1><hello,1><hello,1><hello,1>
	 * <banana,1><banana,1><banana,1><banana,1><banana,1><banana,1>
	 * ���key����һ����ͬ����kv�Ե�key
	 */
	@Override
	protected void reduce(Text text, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
		//mapper������󣬻Ὣ��������Ϊreducer������
		int count = 0;
		for(IntWritable value:values) {
			count += value.get();
		}
		//���Ϊ<key,value>,����valueΪmapper��number�Ļ���
		context.write(new Text(text), new IntWritable(count));
				
	}

}
