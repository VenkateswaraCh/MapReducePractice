package com.cloudwick.hadoop.mapreduce.distributedcache;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DCJoinMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
	private String[] row;
	private static HashMap<String, String> DepartmentMap = new HashMap<String, String>();
	private BufferedReader br;
	/*private String deptName = null;
	private Text txtMapOutputKey = new Text();
	private Text txtMapOutputValue = new Text();*/

	// Employee File
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub

		String line = value.toString();
		String out = null;
		row = line.split(",");
		out = row[0] + "," + row[1]+","+DepartmentMap.get(row[2].toString());
	
		context.write(new IntWritable(Integer.parseInt(row[2])), new Text(out));

	}

	@Override
	protected void setup(Mapper<LongWritable, Text, IntWritable, Text>.Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		Path[] file = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		for (Path path : file) {
			if (path.getName().toString().trim().contains("department.csv")) {
				String line ="";
				try {
					br = new BufferedReader(new FileReader(path.getName().trim().toString()));
					// Read each line, split and load to HashMap
					while ((line = br.readLine()) != null) {
						System.out.println("updating the hash map");
						String dept[] = line.split(",");
						DepartmentMap.put(dept[0], dept[1]);
					}
				} catch (FileNotFoundException e) {
					e.printStackTrace();

				} catch (IOException e) {

					e.printStackTrace();
				} finally {
					if (br != null) {
						br.close();
					}
				}
			}
		}
	}
}