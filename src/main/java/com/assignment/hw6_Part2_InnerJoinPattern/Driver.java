package com.assignment.hw6_Part2_InnerJoinPattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Hello world!
 *
 */


public class Driver 
{
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "ReduceJoin");
		job.setJarByClass(UserJoinMapper.class);

		Path outputDir = new Path(args[2]);

		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, UserJoinMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, BookJoinMapper.class);

		job.setReducerClass(InnerJoinReducer.class);
		job.setNumReduceTasks(1);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, outputDir);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// Delete output if exists
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(outputDir))
			hdfs.delete(outputDir, true);

	
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
