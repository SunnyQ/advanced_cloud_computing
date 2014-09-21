package com.jiaxing.reducer;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import com.jiaxing.global.Global;
import com.jiaxing.utils.Utils;

public class PageRankReducer extends Reducer<Text, Text, Text, Text>{
	private float p = 0;
	private Text outputValue = new Text();
	private StringBuilder sb = new StringBuilder();
	private String node = null;
	private boolean shouldDecode = false;
	@Override
	protected void reduce(Text key, Iterable<Text> values,
			Context context)
			throws IOException, InterruptedException {
		
		for(Text value : values){
			String tempValue = value.toString();
			String[] array = tempValue.toString().split("\\s+", 2);
			if(array[0].equals("-1")){
				if(array.length > 1){
					node = array[1];
				}
				//it indicates that this appears in the original link list because comma can also 
				//exists in the link
				//isNode = true;
			}else{
				p += Float.parseFloat(array[0]);
			}
		}
		
		sb.append(getPageRank(p, 0.85));
		sb.append("\t");
		//if(!shouldDecode){
			sb.append(node == null ? "" : node);
		/*
		}else{
			if(node == null){
				sb.append("");
			}else{
				String[] links = node.split(",");
				for(int i = 0; i < links.length; ++i){
					sb.append(Utils.decodeUrl(links[i]));
					if(i != links.length - 1){
						sb.append(",");
					}
				}
			}
		}*/
		outputValue.set(sb.toString());
		String strKey = key.toString();
		if(strKey.length() > 0){
			if(!shouldDecode){
				context.write(key, outputValue);
			}else{
				//key.set(Utils.decodeUrl(strKey));
				context.write(key, outputValue);
			}
		}
		sb.setLength(0);
		p = 0;
		node = null;
	}
	public double getPageRank(double p, double damp){
		return (1 - damp) + damp * p;
	}
	
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
		shouldDecode = context.getConfiguration().getBoolean(Global.DECODE, false);
	}
}
