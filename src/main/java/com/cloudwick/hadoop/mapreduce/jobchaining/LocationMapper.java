package com.cloudwick.hadoop.mapreduce.jobchaining;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LocationMapper extends Mapper<LongWritable, Text, Text, Text> {
	private String[] row;
	

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String line =value.toString();
		
		row=line.split(",");
		String temp=null;
		
		int salary =Integer.parseInt(row[2]);
		if(salary>5000)
		{
				temp=row[0]+"	"+salary;
				context.write(new Text(row[0]), new Text(temp));
		}
	
}
}