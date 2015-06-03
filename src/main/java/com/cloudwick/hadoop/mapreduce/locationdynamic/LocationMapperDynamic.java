package com.cloudwick.hadoop.mapreduce.locationdynamic;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class LocationMapperDynamic extends Mapper<LongWritable, Text, Text, Text> {
	private String[] set;
	

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String line =value.toString();
		
		set=line.split(",");
		String temp=null;
		Configuration conf = context.getConfiguration();
		String location = conf.get("location");	
		
		if(set[1].equals(location))
		{
				temp=set[1]+"	"+set[2];
				context.write(new Text(set[0]), new Text(temp));
		}
	
}
}