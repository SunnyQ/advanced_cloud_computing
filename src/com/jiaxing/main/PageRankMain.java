package com.jiaxing.main;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.jiaxing.mapper.PageRankMapper;
import com.jiaxing.reducer.PageRankReducer;

public class PageRankMain {
	
	public static final int ITER = 10;
	public static final int NUM_REDUCER = 44;
	public static final int BLOCK_SIZE = 201326592;
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if(otherArgs.length != 4){
			System.err.println("Usage: input output tempdir compression");
			System.exit(2);
		}
		Path inputPath = null, outputPath = null;
		
		for(int i = 1; i <= ITER; ++i){
			conf.setInt("dfs.block.size", BLOCK_SIZE);
			Job job = new Job(conf, "PageRank");
			job.setJarByClass(PageRankMain.class);
			job.setMapperClass(PageRankMapper.class);
			job.setReducerClass(PageRankReducer.class);
			//set to key value input format
			job.setInputFormatClass(KeyValueTextInputFormat.class);
			//set input format to sequence file
			//job.setInputFormatClass(SequenceFileInputFormat.class);
			//job.setOutputFormatClass(SequenceFileOutputFormat.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			//set number of reducers
			job.setNumReduceTasks(NUM_REDUCER);
			
			if(i == 1){
				inputPath = new Path(otherArgs[0]);
			}else{
				inputPath = outputPath;
			}
			FileInputFormat.addInputPath(job, inputPath);
			if(i != ITER){
				outputPath = new Path(otherArgs[2] + i);
			}else{
				outputPath = new Path(otherArgs[1]);
			}
			FileOutputFormat.setOutputPath(job, outputPath);
			
			//set compression
			setCompression(job, args[3]);
			
			job.waitForCompletion(true);
			FileSystem fs = FileSystem.get(URI.create(inputPath.toString()), conf);
			if(i > 1 && fs.exists(inputPath)){
				fs.delete(inputPath);
			}
		}
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
}
