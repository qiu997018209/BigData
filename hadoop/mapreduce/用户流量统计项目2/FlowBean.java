package cn.qiujiahao.bigdata.proviceflow;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class FlowBean implements Writable{

	private long upflow;
	private long downflow;
	private long sumflow;
	
	//�����л�ʱ����Ҫ������ÿղι��캯��������Ҫ��ʾ����һ��
	public FlowBean() {};
	
	public FlowBean(long upflow,long downflow) {
		this.upflow = upflow;
		this.downflow = downflow;
		this.sumflow = upflow + downflow;
	}
	
	//��ȡ��������
	public long getUpflow() {
		return this.upflow;
	}
	//������������
	public void setUpflow(long upflow) {
		this.upflow = upflow;
	}
	
	//��ȡ��������
	public long getDownflow() {
		return this.downflow;
	}
	//������������
	public void setDownflow(long downflow) {
		this.downflow = downflow;
	}	
	//��ȡ�ܵ�����
	public long getSumflow() {
		return this.sumflow;
	}
	//�����ܵ�����
	public void setSumflow(long sumflow) {
		this.sumflow = sumflow;
	}

	@Override
	//��д�����������ݽ��з����л�,��ע�⣬�����л������л�ʱ��˳����ͬ
	public void readFields(DataInput value) throws IOException {
		// TODO Auto-generated method stub
		 upflow = value.readLong();
		 downflow = value.readLong();
		 sumflow = value.readLong();
	}

	@Override
	//��д�����������ݽ������л�
	public void write(DataOutput value) throws IOException {
		// TODO Auto-generated method stub
		value.writeLong(upflow);
		value.writeLong(downflow);
		value.writeLong(sumflow);
	}
	
	@Override
	//��д�ַ�����������Ϊ�����ַ����ķ�ʽ��ʾ
	public String toString() {
		// TODO Auto-generated method stub
		return upflow+"\t"+downflow+"\t"+sumflow+"\t";
	}
	
}
