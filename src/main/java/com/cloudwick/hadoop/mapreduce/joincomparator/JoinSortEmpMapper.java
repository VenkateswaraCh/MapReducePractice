package com.cloudwick.hadoop.mapreduce.joincomparator;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JoinSortEmpMapper extends Mapper<LongWritable, Text, CompositeKey, Text> {
	private static String[] row;
	private static CompositeKey compositekey = new CompositeKey();
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, CompositeKey, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		String line =value.toString();
		String v=null;
		row=line.split(",");
		compositekey.set(new IntWritable(Integer.parseInt(row[2])), new IntWritable(1) );
		v=row[0]+","+row[1];
		context.write(compositekey, new Text(v));
		
	}
		
}
