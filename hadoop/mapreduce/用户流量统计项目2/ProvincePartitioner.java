package cn.qiujiahao.bigdata.proviceflow;

import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * K2  V2  对应的是map输出kv的类型
 * @author
 *
 */
public class ProvincePartitioner extends Partitioner<Text, FlowBean>{
	public static HashMap<String, Integer> Hashdict = new HashMap<String, Integer>();
	static {
		Hashdict.put("136", 0);
		Hashdict.put("137", 1);
		Hashdict.put("138", 2);
		Hashdict.put("139", 3);
	}
	
	@Override
	public int getPartition(Text key, FlowBean value, int numPartitions) {
		String provicenum = key.toString().substring(0, 3);
		Integer index = Hashdict.get(provicenum);
		return (index==null?4:index);
	}

}
