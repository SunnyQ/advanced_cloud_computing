package com.jiaxing.reducer;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PageRankReducer extends Reducer<Text, Text, Text, Text>{
	private float p = 0;
	private Text outputValue = new Text();
	private StringBuilder sb = new StringBuilder();
	private String node = null;
	@Override
	protected void reduce(Text key, Iterable<Text> values,
			Context context)
			throws IOException, InterruptedException {
		for(Text value : values){
			String tempValue = value.toString();
			String[] array = tempValue.toString().split("\\s+");
			if(array[0].equals("-1")){
				node = array[1];
			}else{
				p += Float.parseFloat(array[0]);
			}
		}
		sb.append(p);
		sb.append("\t");
		sb.append(node.toString());
		outputValue.set(sb.toString());
		context.write(key, outputValue);
		sb.setLength(0);
		p = 0;
	}
	
}
