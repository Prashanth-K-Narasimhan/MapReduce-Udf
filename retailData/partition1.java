package retailData;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class partition1 {
	public static class MyMapper extends Mapper<LongWritable,Text,Text,Text>{
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String arr[]=value.toString().split(";");
			
			context.write(new Text(arr[2]),new Text(arr[5]+","+arr[7]+","+arr[8]));
		}
	}
	public static class pa extends Partitioner<Text,Text>{

		@Override
		public int getPartition(Text key, Text arg1, int arg2) {
			if(key.toString().contains("A")){
				return 0;
			}
			if(key.toString().contains("B")){
				return 1;
				
			}
			if(key.toString().contains("C")){
				return 2;
			}if(key.toString().contains("D")){
				return 3;
			}if(key.toString().contains("E")){
				return 4;
			}if(key.toString().contains("F")){
				return 5;
			}if(key.toString().contains("G")){
				return 6;
			}if(key.toString().contains("H")){
				return 7;
			}if(key.toString().contains("I")){
				return 8;
			}if(key.toString().contains("J")){
				return 9;
			}if(key.toString().contains("K")){
				return 10;
			}
			else{
			return 11;
		}}
		
	}
public static class MyReducer extends Reducer<Text,Text,Text,Text>{
	String ss;int abc=0;int max=0;int vol1=0;
		TreeMap tm=new TreeMap();
		public void reduce(Text key,Iterable<Text> value,Context context) throws IOException, InterruptedException{
			
			
			
			for(Text num:value)
			{
				
				
				String str[]=num.toString().split(",");
				
				
				int vol=Integer.parseInt(str[1]);
				int vol1=Integer.parseInt(str[2]);
				
				String id=str[0];
			//	abc=vol1-vol;
			//	max=Math.max(abc, max);
				tm.put((vol1-vol), id);
				
				if(tm.size()>5){
					tm.remove(tm.firstKey());
				}
			}
			
			
			context.write(key, new Text(tm.toString()));
		}
}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration c=new Configuration();
		Job job=Job.getInstance(c,"sss");
		job.setJarByClass(partition1.class);
		job.setMapperClass(MyMapper.class);
		job.setPartitionerClass(pa.class);
		job.setReducerClass(MyReducer.class);
		job.setNumReduceTasks(12);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileSystem.get(c).delete(new Path(args[1]),true);
		FileInputFormat.addInputPath(job, new Path(args[0]));	
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
