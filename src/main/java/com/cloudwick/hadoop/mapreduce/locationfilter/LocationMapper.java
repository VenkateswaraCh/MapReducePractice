package com.cloudwick.hadoop.mapreduce.locationfilter;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class LocationMapper extends Mapper<LongWritable, Text, Text, Text> {
	private String[] set;
	

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String line =value.toString();
		set=line.split(",");
		String temp=null;
		
		
		if(set[1].equals("CA"))
		{
				temp=set[1]+"	"+set[2];
				context.write(new Text(set[0]), new Text(temp));
		}
	
}
}