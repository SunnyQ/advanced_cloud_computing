package com.jiaxing.mapper;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.jiaxing.utils.Parser;
import com.jiaxing.utils.Utils;

public class DataFilterMapper extends Mapper<Text, Text, Text, Text>{
	StringBuilder sb = new StringBuilder();
	List<String> links = null;
	Text outputText = new Text();
	String[] values = null;
	Text outputKey = new Text();
	@Override
	protected void map(Text key, Text value,
			Context context)
			throws IOException, InterruptedException {
		links = Parser.extractLink(value.toString());
		for(int i = 0; i < links.size(); ++i){
			sb.append(links.get(i));
			if(i != links.size() - 1){
				sb.append(",,,,");
			}
		}
		outputText.set(sb.toString());
		//encode url
		outputKey.set(Utils.encodeUrl(key.toString()));
		context.write(key, outputText);
		sb.setLength(0);
	}
}
