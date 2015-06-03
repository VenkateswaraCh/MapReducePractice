package com.cloudwick.hadoop.mapreduce.multipleinputs;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.MultipleInputs;



public class MultiplePaths {
	
	public static void main(String[] args) throws Exception {
	    
		JobConf conf = new JobConf(MultiplePaths.class);
	    conf.setJobName("MultipleMappersPaths");

	    conf.setMapOutputKeyClass(Text.class);
	    conf.setMapOutputValueClass(Text.class);
	    		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		
	    MultipleInputs.addInputPath(conf,new Path(args[0]),TextInputFormat.class,LocationMapper.class);
	    MultipleInputs.addInputPath(conf,new Path(args[1]),TextInputFormat.class,LocationMapperDynamic.class);
	    
	    FileOutputFormat.setOutputPath(conf, new Path(args[2]));
	    JobClient.runJob(conf);

	  }
	
    }

