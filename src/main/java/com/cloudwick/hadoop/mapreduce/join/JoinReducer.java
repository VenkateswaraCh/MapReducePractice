package com.cloudwick.hadoop.mapreduce.join;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class JoinReducer extends Reducer<IntWritable, Text, IntWritable, Text>{

	protected void reduce(IntWritable key, Iterable<Text> values,Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		String line=null;
		String depName=null;
		String[] row;
		String[] r;
		List<String> val = new ArrayList<String>();
		
		for (Text d: values) {
			val.add(d.toString());
		}
		
		
		for (String d : val) {
			if(d.contains("dep"))
			{
				row =d.split(",");
				depName=row[1];
			}
		}
	
		for (String e : val) {
			line =e.toString();
			r=line.split(",");
			if(r[0].equals("emp")){        
			    context.write(new IntWritable(Integer.parseInt(r[1])), new Text(r[2]+"	"+depName));
			}
			
	 }
  
	}
}
