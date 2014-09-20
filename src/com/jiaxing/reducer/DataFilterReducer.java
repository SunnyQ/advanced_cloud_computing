package com.jiaxing.reducer;


import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DataFilterReducer extends Reducer<Text, Text, Text, Text>{
	StringBuilder sb = new StringBuilder();
	Text outputText = new Text();
	int count = 0;
	String deliminator = ",";
	boolean hasLinks = false;
	protected void reduce(Text key, Iterable<Text> values,
			Context context)
			throws IOException, InterruptedException {
		sb.append("1.0\t");
		hasLinks = false;
		for(Text value: values){
			sb.append(value);
			sb.append(",");
			hasLinks = true;
		}
		if(hasLinks){
			sb.setLength(sb.length() - deliminator.length());
		}
		outputText.set(sb.toString());
		context.write(key, outputText);
		sb.setLength(0);
	}
}
