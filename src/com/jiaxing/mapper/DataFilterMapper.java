package com.jiaxing.mapper;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.GenericOptionsParser;

import com.jiaxing.parser.Parser;

public class DataFilterMapper extends Mapper<Text, Text, Text, Text>{
	StringBuilder sb = new StringBuilder();
	List<String> links = null;
	Text keyText = new Text();
	Text outputText = new Text();
	String[] values = null;
	@Override
	protected void map(Text key, Text value,
			Context context)
			throws IOException, InterruptedException {
		System.out.println("key:"+key);
		System.out.println("value:"+value);
		values = value.toString().split(" ");
		links = Parser.extractLink(values[1]);
		for(int i = 0; i < links.size(); ++i){
			sb.append(links.get(i));
			if(i != links.size() - 1){
				sb.append(",");
			}
		}
		keyText.set(values[0]);
		outputText.set(sb.toString());
		context.write(keyText, outputText);
		sb.setLength(0);
	}
}
