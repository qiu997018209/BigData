package cn.qiujiahao.bigdata.mr.flowsum;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class FlowBean implements Writable{

	private long upflow;
	private long downflow;
	private long sumflow;
	
	//反序列化时，需要反射调用空参构造函数，所以要显示定义一个
	public FlowBean() {};
	
	public FlowBean(long upflow,long downflow) {
		this.upflow = upflow;
		this.downflow = downflow;
		this.sumflow = upflow + downflow;
	}
	
	//获取上行流量
	public long getUpflow() {
		return this.upflow;
	}
	//设置上行流量
	public void setUpflow(long upflow) {
		this.upflow = upflow;
	}
	
	//获取下行流量
	public long getDownflow() {
		return this.downflow;
	}
	//设置下行流量
	public void setDownflow(long downflow) {
		this.downflow = downflow;
	}	
	//获取总的流量
	public long getSumflow() {
		return this.sumflow;
	}
	//设置总的流量
	public void setSumflow(long sumflow) {
		this.sumflow = sumflow;
	}

	@Override
	//重写方法，将数据进行反序列化,请注意，反序列化与序列化时的顺序相同
	public void readFields(DataInput value) throws IOException {
		// TODO Auto-generated method stub
		 upflow = value.readLong();
		 downflow = value.readLong();
		 sumflow = value.readLong();
	}

	@Override
	//重写方法，将数据进行序列化
	public void write(DataOutput value) throws IOException {
		// TODO Auto-generated method stub
		value.writeLong(upflow);
		value.writeLong(downflow);
		value.writeLong(sumflow);
	}
	
	@Override
	//重写字符串方法，是为了以字符串的方式显示
	public String toString() {
		// TODO Auto-generated method stub
		return upflow+"\t"+downflow+"\t"+sumflow+"\t";
	}
	
}
