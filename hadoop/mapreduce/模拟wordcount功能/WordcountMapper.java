package cn.qiujiahao.bigdata.mr.wcdemo;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * KEYIN: 默认情况下，是mr框架所读到的一行文本的起始偏移量，Long,
 * 但是在hadoop中有自己的更精简的序列化接口，所以不直接用Long，而用LongWritable
 * 
 * VALUEIN:默认情况下，是mr框架所读到的一行文本的内容，String，同上，用Text
 * 
 * KEYOUT：是用户自定义逻辑处理完成之后输出数据中的key，在此处是单词，String，同上，用Text
 * VALUEOUT：是用户自定义逻辑处理完成之后输出数据中的value，在此处是单词次数，Integer，同上，用IntWritable
 * 
 * @author
 *
 */
public class WordcountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		//将mapreduce传给我们的内容先转成字符串
		String string = value.toString();
		//根据空格将这一行切分成单词
		String[] split = string.split(" ");
		//将单词输出为<单词，1>
		for (String word:split) {
			//将单词作为key，value作为1，以便于后续的数据分发，
			//可以根据单词分发，以便于相同的单词会分给相同的reduce task
			context.write(new Text(word), new IntWritable(1));
		}
	}
}
