package com.cloudwick.hadoop.mapreduce.union;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;



public class UnionReducer extends Reducer<Text, Text, Text, Text>{

	protected void reduce(Text key, Iterable<Text> val,Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		List<String> v = new ArrayList<String>();
		
		for (Text values : val) {
			if(!(v.contains(values.toString()))){
			v.add(values.toString());
			}
		}
		
		context.write(key, new Text(v.get(0)));
		
	}
}
