package cn.qiujiahao.bigdata.mr.wcdemo;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * KEYIN, VALUEIN 对应  mapper输出的KEYOUT,VALUEOUT类型对应
 * 
 * KEYOUT, VALUEOUT 是自定义reduce逻辑处理结果的输出数据类型
 * KEYOUT是单词
 * VLAUEOUT是总次数
 * @author
 *
 */
public class WordcountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	/**
	 * <angelababy,1><angelababy,1><angelababy,1><angelababy,1><angelababy,1>
	 * <hello,1><hello,1><hello,1><hello,1><hello,1><hello,1>
	 * <banana,1><banana,1><banana,1><banana,1><banana,1><banana,1>
	 * 入参key，是一组相同单词kv对的key
	 */
	@Override
	protected void reduce(Text text, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
		//mapper处理完后，会将结果输出作为reducer的输入
		int count = 0;
		for(IntWritable value:values) {
			count += value.get();
		}
		//输出为<key,value>,其中value为mapper中number的汇总
		context.write(new Text(text), new IntWritable(count));
				
	}

}
