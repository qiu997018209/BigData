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



/**���󣺽����������Ʒ��ϵ�һ��
order.txt(����id, ����, ��Ʒ���, ����)
		  1001	20150710	P0001	2
		  1002	20150710	P0001	3
		  1002	20150710	P0002	3
		  1003	20150710	P0003	3
product.txt(��Ʒ���, ��Ʒ����, �۸�, ����)
			P0001	С��5	1001	2
			P0002	����T1	1000	3
			P0003	����T2	1002	4
			
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
			
			//ÿһ�����붼��һ����Ƭ��������ͼƬ��Ҳ���������ݿ⣬�˴�ǿת���ļ�
			FileSplit split = (FileSplit)context.getInputSplit();
			//��ȡ���ļ���
			String name = split.getPath().getName();
			String pid = "";
			if(name.startsWith("order")) {
				//˵�����ļ��Ƕ����ļ�
				pid = val[2];
				Bean.set(Integer.parseInt(val[0]), val[1], val[2], Integer.parseInt(val[3]), "", 0, 0, "order");
			}
			else {
				//˵���ǲ�Ʒ�ļ�
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
		//�˴�ִ�е�keyһ����ͬ��Ҳ����˵��Ʒ�����ͬ
		@Override
		protected void reduce(Text text, Iterable<InfoBean> value, Context context)
				throws IOException, InterruptedException {

			for(InfoBean bean:value) {
				String flag = bean.getFlag();
				if("order".equals(flag)) {
					//˵���Ƕ����ļ�
					try {
						BeanUtils.copyProperties(myBean, bean);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
				else {
					//˵���ǲ�Ʒ�ļ�
					try {
						BeanUtils.copyProperties(tempBean, bean);
						dbBean.add(tempBean);
					} catch (Exception e) {
						e.printStackTrace();
					}
					
					
				}
				
			}
			//�ڴ˴�������װ
			for(InfoBean bean2:dbBean) {
				myBean.setPname(bean2.getPname());
				myBean.setCategory_id(bean2.getCategory_id());
				myBean.setPrice(bean2.getPrice());
				myBean.setFlag("�ϲ������Ϣ��");
			}
			
			context.write(myBean, NullWritable.get());
		}
		
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
					
		Job job = Job.getInstance(conf);

		// ָ���������jar�����ڵı���·��
		// job.setJar("c:/join.jar");

		job.setJarByClass(RJoin.class);
		// ָ����ҵ��jobҪʹ�õ�mapper/Reducerҵ����
		job.setMapperClass(RJoinMapper.class);
		job.setReducerClass(RJoinReducer.class);

		// ָ��mapper������ݵ�kv����
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(InfoBean.class);

		// ָ��������������ݵ�kv����
		job.setOutputKeyClass(InfoBean.class);
		job.setOutputValueClass(NullWritable.class);

		// ָ��job������ԭʼ�ļ�����Ŀ¼
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		// ָ��job������������Ŀ¼
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// ��job�����õ���ز������Լ�job���õ�java�����ڵ�jar�����ύ��yarnȥ����
		/* job.submit(); */
		boolean res = job.waitForCompletion(true);
		System.exit(res ? 0 : 1);

	
}
	
}
