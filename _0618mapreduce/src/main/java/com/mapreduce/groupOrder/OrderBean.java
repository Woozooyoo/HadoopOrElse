package com.mapreduce.groupOrder;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class OrderBean implements WritableComparable<OrderBean> {

	private int orderId; // 订单id
	private Double price; // 商品价格

	public OrderBean() {
		super();
	}

	public OrderBean(int orderId, Double price) {
		super();
		this.orderId = orderId;
		this.price = price;
	}

	public int getOrderId() {
		return orderId;
	}

	public void setOrderId(int orderId) {
		this.orderId = orderId;
	}

	public Double getPrice() {
		return price;
	}

	public void setPrice(Double price) {
		this.price = price;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(orderId);
		out.writeDouble(price);
	}
	
	
	@Override
	public void readFields(DataInput in) throws IOException {
		this.orderId = in.readInt();
		this.price = in.readDouble();

	}

	@Override
	public int compareTo(OrderBean o) {
		//  两次排序
		// 1 按照id号排序
		int comResult;
		
		if (orderId>o.getOrderId()) {
			comResult=1;
		}else if(orderId<o.getOrderId()){
			comResult=-1;
		}else{
			// 2 按照价格倒序排序
			comResult = this.price > o.getPrice()?-1:1;
		}
		
		return comResult;
	}

	@Override
	public String toString() {
		return orderId + "\t" + price;
	}
}
