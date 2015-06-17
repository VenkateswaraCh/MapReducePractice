package com.cloudwick.hadoop.mapreduce.counters;


import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class CounterMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
	private String[] row;
	
	enum MYCOUNTER {
		INFO, WARN, DEBUG, FATAL
		}
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		if (value.toString().contains("INFO"))
			context.getCounter(MYCOUNTER.INFO).increment(1);
		else if(value.toString().contains("WARN"))
			context.getCounter(MYCOUNTER.WARN).increment(1);
		else if(value.toString().contains("DEBUG"))
			context.getCounter(MYCOUNTER.DEBUG).increment(1);
		else
			context.getCounter(MYCOUNTER.FATAL).increment(1);
		
		String line = value.toString();
		String out = null;
		row = line.split(",");
		out = row[0] + "," + row[1];
	
		context.write(new IntWritable(Integer.parseInt(row[2])), new Text(out+",SUCCESS"));

	}
}