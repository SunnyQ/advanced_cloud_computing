package com.jiaxing.main;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.jiaxing.mapper.DataFilterMapper;
import com.jiaxing.mapper.PageRankMapper;
import com.jiaxing.reducer.DataFilterReducer;
import com.jiaxing.reducer.PageRankReducer;

public class PageRankMain {
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if(otherArgs.length != 2){
			System.err.println("Usage: input output");
			System.exit(2);
		}
		FileSystem fs = FileSystem.get(conf);
		for(int i = 1; i <= 10; ++i){
			Job job = new Job(conf, "PageRank");
			job.setJarByClass(PageRankMain.class);
			job.setMapperClass(PageRankMapper.class);
			job.setReducerClass(PageRankReducer.class);
			//set input format to sequence file
			//job.setInputFormatClass(T.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			Path inputPath;
			if(i == 1){
				inputPath = new Path(otherArgs[0]);
			}else{
				inputPath = new Path(otherArgs[1] + (i - 1));
			}
			FileInputFormat.addInputPath(job, inputPath);
			FileOutputFormat.setOutputPath(job, new Path(otherArgs[1] + i));
			job.waitForCompletion(true);
			/*
			if(i > 1 && fs.exists(inputPath)){
				fs.delete(inputPath);
			}
			*/
			
		}
		
	}
}
