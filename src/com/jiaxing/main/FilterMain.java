package com.jiaxing.main;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.jiaxing.mapper.DataFilterMapper;
import com.jiaxing.reducer.DataFilterReducer;

public class FilterMain {
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if(otherArgs.length != 4){
			System.err.println("Usage: input output compression range");
			System.exit(2);
		}
		
		Job job = new Job(conf, "DataFilter");
		job.setJarByClass(FilterMain.class);
		job.setMapperClass(DataFilterMapper.class);
		job.setReducerClass(DataFilterReducer.class);
		//set input format to sequence file
		job.setInputFormatClass(SequenceFileInputFormat.class);
		//job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		//set input and output path
		//add many paths
		addPaths(job, otherArgs[0], Integer.parseInt(otherArgs[3]));
		//FileInputFormat.setInputPaths(job, otherArgs[0]);
		
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		//set compressoin format
		setCompression(job, otherArgs[2]);
		//start job
		System.exit(job.waitForCompletion(true)? 0 : 1);
		
	}
	
	public static void setCompression(Job job, String compression){
		//set compression format
		if(compression.equals("zlib")){
			FileOutputFormat.setCompressOutput(job, true);
			FileOutputFormat.setOutputCompressorClass(job, DefaultCodec.class);
		}else
			if(compression.equals("bzip2")){
				FileOutputFormat.setCompressOutput(job, true);
				FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);
			}
			else
				if(compression.equals("snappy")){
					FileOutputFormat.setCompressOutput(job, true);
					FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
				}
				else{
					//do not use compression
				}
	}
	
	public static void addPaths(Job job, String prefix, int max) throws IOException{
		for(int i = 0; i <= max; ++i){
			String inpath = prefix + "metadata-000" + (i < 10 ? "0" + i : i);
			FileInputFormat.addInputPaths(job, inpath);
		}
	}
}
