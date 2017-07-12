package project_main;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Q1a {
	public static class MapClass extends Mapper<LongWritable,Text,Text,Text>
	   {
	      public void map(LongWritable key, Text value, Context context)
	      {	    
	      try{ 
	    		String record = value.toString();
	    		String[] parts = record.split("\\t");
	    		
	    	//	if(parts[4].equalsIgnoreCase("DATA ENGINEER"))
	    	//	{
	    		context.write(new Text(parts[7]), new Text(parts[4]));
	    	//	}
	    	}
	         catch(Exception e)
	         {

	        	 System.out.println(e.getMessage());
	      
	         }
	      }
	   }
	 public static class ReduceClass extends Reducer<Text, Text, Text, LongWritable> {
		 HashMap<String,Integer> hm=new HashMap<String,Integer>(); 
			 Text keys = new Text();
		    public void reduce(Text key, Iterable<Text> values, Context context) 
		      throws IOException, InterruptedException {
		    	
				int count = 0;
		    	
		        for (Text val : values) {
		        	String jobtitle = val.toString();
		        	if(jobtitle.equalsIgnoreCase("DATA ENGINEER"))
		        	{
		        		count++;
		        	}
		         }
		        String year = key.toString();
		        hm.put(year, count);
		  
		        context.write(key, new LongWritable(count));
		        }
		    
	 }
	 	
	  public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();
		    //conf.set("name", "value")
		    
		    Job job = Job.getInstance(conf, "Count");
		    job.setJarByClass(Q1a.class);
		    job.setMapperClass(MapClass.class);
		    //job.setCombinerClass(ReduceClass.class);
		    job.setReducerClass(ReduceClass.class);
		    //job.setNumReduceTasks(0);
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(Text.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(LongWritable.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
}
