package wordCount;

import java.io.IOException;
import org.apache.hadoop.conf.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Find{

 public static class Map extends Mapper<LongWritable, Text, Text, LongWritable> {
   // private final static LongWritable one = new LongWritable(1);
    public Text word = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	String search = context.getConfiguration().get("mytext");
        String line = value.toString();
        	if(line.contains(search))
        	{
        		word = new Text(line);
        		context.write(word,new LongWritable(1));
        	}
     }
 }
 
 public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    	if(args.length > 2)
	    	{
	    		conf.set("mytext", args[2]);    	    
	    	}
	    	else
	    	{
	    		System.out.println("Retry");
	    		System.exit(0);
	    	}
	    
	    Job job = Job.getInstance(conf, "Word Find");
	    job.setJarByClass(Find.class);
	    job.setMapperClass(Map.class);
	    //job.setCombinerClass(ReduceClass.class);
	    //job.setNumReduceTasks(1);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(LongWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
 }