package com.cloudwick.hadoop.mapreduce.joincomparator;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinSortReducer extends Reducer<CompositeKey, Text, Text, Text> {

	protected void reduce(CompositeKey key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub

		String depName = null;

		int i=0;
		for (Text val : values) 	{
			if(i==0){
				depName=val.toString();
				i++;
			}		
			else
			context.write(new Text(key.getId().toString()+","+val.toString()),new Text(depName));
		}

	}

}
