package com.cloudwick.hadoop.mapreduce.joincomparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;


public class CompositeKey implements WritableComparable<CompositeKey> {
	
	IntWritable id;
	IntWritable tag;
	
	public CompositeKey() {
		set(new IntWritable(), new IntWritable());
	}
	
	public CompositeKey(Integer id, Integer tag) {
		set(new IntWritable(id), new IntWritable(tag));
	}
	
	public void set(IntWritable id, IntWritable tag) {
		// TODO Auto-generated method stub
		this.id = id;
		this.tag = tag;
	}
	

	public IntWritable getId() {
		return id;
	}

	public void setId(IntWritable id) {
		this.id = id;
	}

	public IntWritable getTag() {
		return tag;
	}

	public void setTag(IntWritable tag) {
		this.tag = tag;
	}

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		id.readFields(in);
		tag.readFields(in);
		
	}

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		id.write(out);
		tag.write(out);
	}

	public int compareTo(CompositeKey ep) {
		// TODO Auto-generated method stub
		int cmp = id.compareTo(ep.id);
		if (cmp != 0) {
			return cmp;
		}
		return tag.compareTo(ep.tag);
	}

	
}