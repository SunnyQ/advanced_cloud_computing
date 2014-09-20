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
		String[] array = value.toString().trim().split("\\s+", 3);
		if(array.length > 2 && array[2].length() > 0){
			float p = Float.parseFloat(array[1]);
			String[] outputLinks = array[2].split(",");
			outputValue.set(String.valueOf(p / outputLinks.length));
			for(String link : outputLinks){
				outputKey.set(link);
				context.write(outputKey, outputValue);
			}
		}
		outputKey.set(array[0]);
		if(array.length > 2){
			outputValue.set("-1\t" + array[2]);
		}else{
			outputValue.set("-1\t");
		}
		context.write(outputKey, outputValue);//kep the structure of the network
	}
}
