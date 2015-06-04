package com.cloudwick.hadoop.mapreduce.jobchaining;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class JobChain extends Configured implements Tool {
	
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if(args.length!=2)
		{			
			System.out.printf("Usage: %s [generic options] <input dir> <output dir>\n");
			//return -1;
		}
		
		
		Configuration conf = new Configuration();
		ControlledJob cJob = new ControlledJob(conf);
		cJob.setJobName("Location");
		Job loc = cJob.getJob();
        loc.setJarByClass(JobChain.class);
        loc.setMapperClass(LocationFilterMapper.class);
        
        loc.setMapOutputKeyClass(Text.class);
        loc.setMapOutputValueClass(Text.class);
        loc.setOutputKeyClass(Text.class);
        loc.setOutputValueClass(Text.class);
        
        FileInputFormat.setInputPaths(loc, new Path(args[0]));
        FileOutputFormat.setOutputPath(loc, new Path("/user/venkat/j"));
     
        loc.setNumReduceTasks(0);
        loc.waitForCompletion(true);       
        if (loc.waitForCompletion(true)) {
            return 0;
        }
        	
        Configuration salConf = new Configuration();
		//String[] salArgs = new GenericOptionsParser(salConf, args).getRemainingArgs();
		ControlledJob csal = new ControlledJob(salConf);
		csal.setJobName("SalaryFilter");
				
		Job sal = csal.getJob();
        sal.setJarByClass(JobChain.class);
        sal.setMapperClass(LocationMapper.class);
        sal.setMapOutputKeyClass(Text.class);
        sal.setMapOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(sal, new Path("/user/venkat/j/part*"));
        FileOutputFormat.setOutputPath(sal, new Path(args[1]));
        sal.setNumReduceTasks(0);
        //sal.waitForCompletion(true);
        
        if (sal.waitForCompletion(true)) {
            return 0;
        }
        /*
        JobControl jobctrl = new JobControl("locsalary");
		
        jobctrl.addJob(cJob);
        csal.addDependingJob(cJob);
		jobctrl.addJob(csal);
		
        
		jobctrl.run();
		if(jobctrl.)
			jobctrl.stop();
		*/
		return 1;
       
		
		
	}
	public static void main(String[] args) throws Exception {
		
		int exitCode = ToolRunner.run(new JobChain(), args);
		
        System.exit(exitCode);
       
        
    }

}

