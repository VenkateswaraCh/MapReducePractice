package com.cloudwick.hadoop.mapreduce.multipleinputs;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class LocationMapper extends MapReduceBase  implements Mapper<LongWritable, Text, Text, Text> {
	
	private String[] set;
	
	public void map(LongWritable key, Text value,
			OutputCollector<Text, Text> output, Reporter report) throws IOException {
		// TODO Auto-generated method stub
		String line =value.toString();
		set = line.split(",");
		String temp=null;
		if(set[1].equals("CA"))
		{
				temp=set[1]+"	"+set[2];
				output.collect(new Text(set[0]), new Text(temp));
				
		}
		
	}

	
}