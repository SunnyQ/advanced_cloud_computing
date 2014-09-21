package com.jiaxing.mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class PageRankMapper extends Mapper<Text, Text, Text, Text>{
	private Text outputKey = new Text();
	private Text outputValue = new Text();
	@Override
	protected void map(Text key, Text value,
			Context context)
			throws IOException, InterruptedException {
		String[] array = value.toString().trim().split("\\s+", 2);
		if(array.length > 1 && array[1].length() > 0){
			float p = Float.parseFloat(array[0]);
			String[] outputLinks = array[1].split(",");
			outputValue.set(String.valueOf(p / outputLinks.length));
			for(String link : outputLinks){
				outputKey.set(link);
				context.write(outputKey, outputValue);
			}
		}
		outputKey = key;
		if(array.length > 1){
			outputValue.set("-1\t" + array[1]);
		}else{
			outputValue.set("-1\t");
		}
		context.write(outputKey, outputValue);//kep the structure of the network
	}
}
