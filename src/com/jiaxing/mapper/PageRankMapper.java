package com.jiaxing.mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class PageRankMapper extends Mapper<LongWritable, Text, Text, Text>{
	private Text outputKey = new Text();
	private Text outputValue = new Text();
	@Override
	protected void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {
		String[] array = value.toString().split("\\s+", 3);
		
		float p = Float.parseFloat(array[1]);
		String[] links = array[2].split(",");
		outputValue.set(String.valueOf(p / links.length));
		for(String link : links){
			outputKey.set(link);
			context.write(outputKey, outputValue);
			System.out.println("---------------");
			System.out.println(outputKey.toString());
			System.out.println(outputValue.toString());
			System.out.println("---------------");
		}
		outputKey.set(array[0]);
		outputValue.set("-1\t" + array[2]);
		context.write(outputKey, outputValue);//kep the structure of the network
	}
	
}
