package com.cloudwick.hadoop.mapreduce.counters;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import com.cloudwick.hadoop.mapreduce.counters.CounterMapper.MYCOUNTER;

public class Counter extends Configured implements Tool {
	private final Log LOG = LogFactory.getLog(Counter.class);
    public int run(String[] args) throws Exception {

        if (args.length != 2 ) {
            System.out.printf(
                    "Usage: %s [generic options] <input dir> <output dir>\n", getClass()
                    .getSimpleName());
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }
        Configuration conf = new Configuration();
        
        Job job = new Job(conf);
        
        job.setJarByClass(Counter.class);
        job.setJobName(this.getClass().getName());
              
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        
     
                
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(CounterMapper.class);
        job.setNumReduceTasks(0);
        if (job.waitForCompletion(true)) {
        	
            return 0;
        }
        LOG.info("INFO COUNT: "
				+ job.getCounters().findCounter(MYCOUNTER.INFO).getValue());
		LOG.info("WARN COUNT: "
				+ job.getCounters().findCounter(MYCOUNTER.WARN)
						.getValue());
		LOG.info("DEBUG COUNT: "
				+ job.getCounters().findCounter(MYCOUNTER.DEBUG).getValue());
		LOG.info("FATAL COUNT: "
				+ job.getCounters().findCounter(MYCOUNTER.FATAL).getValue());
		
        return 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Counter(), args);
        System.exit(exitCode);
    }
}