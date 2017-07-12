package day2MapSideJoin;

import java.io.IOException;

import java.net.URI;

import java.net.URISyntaxException;


import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.filecache.DistributedCache;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.DoubleWritable;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import day2MapSideJoin.myMapper;
import day2MapSideJoin.myReducer;


@SuppressWarnings("deprecation")
public class myDriver  {
	
public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException{
	
Job j = new Job(new Configuration(), "Mapper Side Join/ In Memory Join");
	
j.setJarByClass(myDriver.class);
	
j.setMapperClass(myMapper.class);
	
j.setReducerClass(myReducer.class);
	
j.setNumReduceTasks(1);
	
j.setMapOutputKeyClass(Text.class);
	
j.setMapOutputValueClass(DoubleWritable.class);
	
	
FileInputFormat.addInputPath(j,new Path(args[0]));
	
FileOutputFormat.setOutputPath(j, new Path(args[1]));
	
	
DistributedCache.addCacheFile(new URI("/Day2/file1"),j.getConfiguration());

	
	
System.exit(j.waitForCompletion(true)?0:1);
	
}
}
