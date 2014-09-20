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
		String[] array = value.toString().trim().split("\\s+", 2);
		String[] links = array[1].split(",,,,", 2);
		if(links.length > 1 && links[1].length() > 0){
			float p = Float.parseFloat(links[0]);
			String[] outputLinks = links[1].split(",,,,");
			outputValue.set(String.valueOf(p / outputLinks.length));
			for(String link : outputLinks){
				outputKey.set(link);
				context.write(outputKey, outputValue);
			}
		}
		outputKey.set(array[0]);
		if(links.length > 1){
			outputValue.set("-1,,,," + links[1]);
		}else{
			outputValue.set("-1,,,,");
		}
		context.write(outputKey, outputValue);//kep the structure of the network
	}
}
