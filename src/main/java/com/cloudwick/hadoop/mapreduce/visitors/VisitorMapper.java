package com.cloudwick.hadoop.mapreduce.visitors;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

public class VisitorMapper extends
		Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String line = value.toString();
		String[] arr=line.split(",");
		
		//IntWritable val= new IntWritable(1);
		
			context.write(new Text(arr[1]), new Text(arr[0]));
	
		
				
		
	}
}
