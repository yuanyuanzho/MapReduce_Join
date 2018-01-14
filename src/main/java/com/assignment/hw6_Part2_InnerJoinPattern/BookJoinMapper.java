package com.assignment.hw6_Part2_InnerJoinPattern;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class BookJoinMapper extends Mapper<Object, Text, Text, Text>{
	private Text outKey = new Text();
	private Text outValue = new Text();
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String[] fields = value.toString().split(";");
		String userId = fields[0];
		
		outKey.set(userId);
		outValue.set("B" + value.toString());
		context.write(outKey, outValue);
	}
}
