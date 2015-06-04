package com.cloudwick.hadoop.mapreduce.join;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JoinMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
	private String[] row;
	
//Employee File
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		String line =value.toString();
		String out=null;
		row=line.split(",");
		out="emp,"+row[0]+","+row[1];
		context.write(new IntWritable(Integer.parseInt(row[2])), new Text(out));
		
	
}
}