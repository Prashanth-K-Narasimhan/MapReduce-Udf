package retailData;

import java.io.IOException;
//import java.text.DateFormat;
//import java.util.*;

import org.apache.hadoop.conf.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MapR {
 public static class Map extends Mapper<LongWritable, Text, NullWritable, LongWritable> {

    //private Text C_id = new Text();
    private LongWritable Amount = new LongWritable();
    
    long Date_Time;
    String Cust_ID;
	String Age;
	String Res_Area;
	String Category;
	String Prod_ID;
	int Qty;
	int Total_Cost;
	int Total_Sales;	
	String[] data;
    
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        data = line.split(";");
        
		//Date_Time = Long.parseLong(data[0]);
		//Date date = new Date(Date_Time);
		//String dateNew = DateFormat.getInstance().format(date).toString();
		
		Cust_ID = data[1];
		Age = data[2];
		Res_Area = data[3];
		Category = data[4];
		Prod_ID = data[5];
		Qty = Integer.parseInt(data[6]);
		Total_Cost = Integer.parseInt(data[7]);
		Total_Sales = Integer.parseInt(data[8]);
		
		//C_id.set(Cust_ID);
		Amount.set(Total_Sales);
		
		context.write(NullWritable.get(), Amount);
		}
    }

 public static class Reduce extends Reducer<NullWritable, LongWritable, NullWritable, LongWritable> {

	    public void reduce(Text key, Iterable<LongWritable> values, Context context) 
	      throws IOException, InterruptedException {
	        long num = 0;
	        long ref = 0;
	        for (LongWritable val : values) {
	        	num = val.get();
	        	
	        	if(num >= ref)
	        	{
	        		ref = num;
	        	}
	            
	        }
	        context.write(NullWritable.get(), new LongWritable(ref));
	    }
	 }

 public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    //conf.set("name", "value")
	    
	    Job job = Job.getInstance(conf, "Retail_Data");
	    job.setJarByClass(MapR.class);
	    job.setMapperClass(Map.class);
	    //job.setCombinerClass(ReduceClass.class);
	    job.setNumReduceTasks(4);
	    job.setReducerClass(Reduce.class);
	    job.setOutputKeyClass(NullWritable.class);
	    job.setOutputValueClass(LongWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
 }