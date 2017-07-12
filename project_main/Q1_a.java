package project_main;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Q1_a {
	public static class MapClass extends Mapper<LongWritable,Text,Text,Text>
	   {
	      public void map(LongWritable key, Text value, Context context)
	      {	    
	      try{ 
	    		String record = value.toString();
	    		String[] parts = record.split("\\t");
	    		
	    		if(parts[4].equalsIgnoreCase("DATA ENGINEER"))
	    		{
	    		context.write(new Text(parts[4]), new Text(parts[7]));
	    		}
	    	}
	         catch(Exception e)
	         {

	        	 System.out.println(e.getMessage());
	      
	         }
	      }
	   }
	 public static class ReduceClass extends Reducer<Text, Text, Text, LongWritable> {
		 
			 Text keys = new Text();
		    public void reduce(Text key, Iterable<Text> values, Context context) 
		      throws IOException, InterruptedException {
		    	
				long y2011 = 0;
				long y2012 = 0;
				long y2013 = 0;
				long y2014 = 0;
				long y2015 = 0;
				long y2016 = 0;
		    	
		        for (Text val : values) {
		        	String year = val.toString();
		        	if(year.equalsIgnoreCase("2011"))
		        	{
		        		y2011++;
		        	}
		        	if(year.equalsIgnoreCase("2012"))
		        	{
		        		y2012++;
		        	}
		        	if(year.equalsIgnoreCase("2013"))
		        	{
		        		y2013++;
		        	}
		        	if(year.equalsIgnoreCase("2014"))
		        	{
		        		y2014++;
		        	}
		        	if(year.equalsIgnoreCase("2015"))
		        	{
		        		y2015++;
		        	}
		        	if(year.equalsIgnoreCase("2016"))
		        	{
		        		y2016++;
		        	}
		        	
		         }
		       
		        long growth_factor = (y2012-y2011)+(y2013-y2012)+(y2014-y2013)+(y2015-y2014)+(y2016-y2015);
		 		  
		        context.write(key, new LongWritable(growth_factor));
		        }
		    
	 }
	 	
	  public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();
		    //conf.set("name", "value")
		    
		    Job job = Job.getInstance(conf, "Count");
		    job.setJarByClass(Q1_a.class);
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
