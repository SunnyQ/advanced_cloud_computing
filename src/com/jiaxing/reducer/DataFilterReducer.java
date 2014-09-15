package com.jiaxing.reducer;


import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DataFilterReducer extends Reducer<Text, Text, Text, Text>{
	StringBuilder sb = new StringBuilder();
	Text outputText = new Text();
	int count = 0;
	protected void reduce(Text key, Iterable<Text> values,
			Context context)
			throws IOException, InterruptedException {
		sb.append("1.0\t");
		for(Text value: values){
			sb.append(value);
			sb.append(",");
		}
		sb.setLength(sb.length() - 1);
		outputText.set(sb.toString());
		context.write(key, outputText);
		sb.setLength(0);
		++count;
		if(count == 100){
			System.exit(0);
		}
	}
}
