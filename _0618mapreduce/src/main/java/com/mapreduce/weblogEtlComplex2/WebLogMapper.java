package com.mapreduce.weblogEtlComplex2;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WebLogMapper extends Mapper<LongWritable, Text, WebLogBean, NullWritable>{
	WebLogBean logBean = new WebLogBean();
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// 1 获取1行
		String line = value.toString();
		
		// 2 解析日志是否合法
		pressLog(line,context);
		
//		if (!bean.isValid()) {
//			return;
//		}
		
		
		// 3 输出
		context.write(logBean, NullWritable.get());
	}

	// 解析日志
	public void pressLog(String line, Context context) {
//		WebLogBean logBean = new WebLogBean();
		
		// 1 截取
		String[] fields = line.split(" ");
		
		if (fields.length > 11) {
			// 2封装数据
			logBean.setRemote_addr(fields[0]);
			logBean.setRemote_user(fields[1]);
			logBean.setTime_local(fields[3].substring(1));
			logBean.setRequest(fields[6]);
			logBean.setStatus(fields[8]);
			logBean.setBody_bytes_sent(fields[9]);
			logBean.setHttp_referer(fields[10]);
			
			if (fields.length > 12) {
				logBean.setHttp_user_agent(fields[11] + " "+ fields[12]);
			}else {
				logBean.setHttp_user_agent(fields[11]);
			}
			
			// 大于400，HTTP错误
			if (Integer.parseInt(logBean.getStatus()) >= 400) {
				logBean.setValid(false);
				context.getCounter("map", "false").increment(1);
			}else{
				logBean.setValid(true);
				context.getCounter("map", "true").increment(1);
			}
		}else {
			logBean.setValid(false);
			context.getCounter("map", "false").increment(1);
		}
		
//		return logBean;
	}
}
