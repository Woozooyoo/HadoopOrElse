package com.atguigu.mapreduce.reducejoin;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class TableBean implements WritableComparable<TableBean> {

	private String order_id; // 订单id
	private String pid; // 产品id
	private int amount; // 产品数量

	private String pname; // 产品名称
	private String flag;// 表的标记

	public TableBean() {
		super();
	}

	public TableBean(String order_id, String pid, int amount, String pname, String flag) {
		super();
		this.order_id = order_id;
		this.pid = pid;
		this.amount = amount;
		this.pname = pname;
		this.flag = flag;
	}

	public String getOrder_id() {
		return order_id;
	}

	public void setOrder_id(String order_id) {
		this.order_id = order_id;
	}

	public String getPid() {
		return pid;
	}

	public void setPid(String pid) {
		this.pid = pid;
	}

	public int getAmount() {
		return amount;
	}

	public void setAmount(int amount) {
		this.amount = amount;
	}

	public String getPname() {
		return pname;
	}

	public void setPname(String pname) {
		this.pname = pname;
	}

	public String getFlag() {
		return flag;
	}

	public void setFlag(String flag) {
		this.flag = flag;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(order_id);
		out.writeUTF(pid);
		out.writeInt(amount);
		out.writeUTF(pname);
		out.writeUTF(flag);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.order_id = in.readUTF();
		this.pid = in.readUTF();
		this.amount = in.readInt();
		this.pname = in.readUTF();
		this.flag = in.readUTF();
	}

	@Override
	public String toString() {
		return order_id + "\t" + pname + "\t" + amount +"\t";
	}

	@Override
	public int compareTo(TableBean o) {
		
		return this.order_id.compareTo(o.getOrder_id());
	}
}
