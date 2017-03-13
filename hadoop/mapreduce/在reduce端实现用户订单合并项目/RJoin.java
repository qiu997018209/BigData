package cn.qiujiahao.bigdata.mr.rjoin;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.RollingUpgradeInfo.Bean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.sun.corba.se.impl.javax.rmi.CORBA.Util;



/**需求：将订单表和商品表合到一起
order.txt(订单id, 日期, 商品编号, 数量)
		  1001	20150710	P0001	2
		  1002	20150710	P0001	3
		  1002	20150710	P0002	3
		  1003	20150710	P0003	3
product.txt(商品编号, 商品名字, 价格, 数量)
			P0001	小米5	1001	2
			P0002	锤子T1	1000	3
			P0003	锤子T2	1002	4
			
 * @author qiujiahao
 *
 */
public class RJoin {
	
	static class RJoinMapper extends Mapper<LongWritable, Text, Text, InfoBean> {
		
		InfoBean Bean = new InfoBean(); 
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String[] val = value.toString().split("\t");
			
			//每一个输入都是一个分片，可以是图片，也可以是数据库，此处强转成文件
			FileSplit split = (FileSplit)context.getInputSplit();
			//获取到文件名
			String name = split.getPath().getName();
			String pid = "";
			if(name.startsWith("order")) {
				//说明本文件是订单文件
				pid = val[2];
				Bean.set(Integer.parseInt(val[0]), val[1], val[2], Integer.parseInt(val[3]), "", 0, 0, "order");
			}
			else {
				//说明是产品文件
				pid = val[0];
				Bean.set(0, "", val[0], 0, val[1], Integer.parseInt(val[3]), Float.parseFloat(val[2]), "product");
			}
			context.write(new Text(pid), Bean);
			
		}
				
	}
	
	static class RJoinReducer extends Reducer<Text, InfoBean, InfoBean, NullWritable> {
		
		InfoBean myBean = new InfoBean();
		InfoBean tempBean = new InfoBean();
		ArrayList<InfoBean> dbBean = new ArrayList<InfoBean>(); 
		//此处执行的key一定相同，也就是说商品编号相同
		@Override
		protected void reduce(Text text, Iterable<InfoBean> value, Context context)
				throws IOException, InterruptedException {

			for(InfoBean bean:value) {
				String flag = bean.getFlag();
				if("order".equals(flag)) {
					//说明是订单文件
					try {
						BeanUtils.copyProperties(myBean, bean);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
				else {
					//说明是产品文件
					try {
						BeanUtils.copyProperties(tempBean, bean);
						dbBean.add(tempBean);
					} catch (Exception e) {
						e.printStackTrace();
					}
					
					
				}
				
			}
			//在此处进行组装
			for(InfoBean bean2:dbBean) {
				myBean.setPname(bean2.getPname());
				myBean.setCategory_id(bean2.getCategory_id());
				myBean.setPrice(bean2.getPrice());
				myBean.setFlag("合并后的信息表");
			}
			
			context.write(myBean, NullWritable.get());
		}
		
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
					
		Job job = Job.getInstance(conf);

		// 指定本程序的jar包所在的本地路径
		// job.setJar("c:/join.jar");

		job.setJarByClass(RJoin.class);
		// 指定本业务job要使用的mapper/Reducer业务类
		job.setMapperClass(RJoinMapper.class);
		job.setReducerClass(RJoinReducer.class);

		// 指定mapper输出数据的kv类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(InfoBean.class);

		// 指定最终输出的数据的kv类型
		job.setOutputKeyClass(InfoBean.class);
		job.setOutputValueClass(NullWritable.class);

		// 指定job的输入原始文件所在目录
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		// 指定job的输出结果所在目录
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// 将job中配置的相关参数，以及job所用的java类所在的jar包，提交给yarn去运行
		/* job.submit(); */
		boolean res = job.waitForCompletion(true);
		System.exit(res ? 0 : 1);

	
}
	
}
