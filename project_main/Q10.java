package project_main;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
;

public class Q10 {
	public static class MapClass extends Mapper<LongWritable,Text,Text,Text>
	   {
		
	      public void map(LongWritable key, Text value, Context context)
	      {	    
	      try{ 
	    		String record = value.toString();
	    		String[] parts = record.split("\\t");
	    		context.write(new Text(parts[4]), new Text(parts[1]));
	         }
	         catch(Exception e)
	         {

	        	 System.out.println(e.getMessage());
	      
	         }
	      }
	   }
	 public static class ReduceClass extends Reducer<Text, Text, Text, DoubleWritable> {

			 Text keys = new Text();
		    public void reduce(Text key, Iterable<Text> values, Context context) 
		      throws IOException, InterruptedException {
		    	
				long total = 0;
				long certified = 0;
				long c_withdrawn = 0;
		    	
		        for (Text val : values) {
		        	String case_status = val.toString();
		           if(case_status.equalsIgnoreCase("CERTIFIED"))
		           {
		        	   total++;
		        	   certified++;
		           }
		           else if(case_status.equalsIgnoreCase("CERTIFIED-WITHDRAWN"))
		           {
		        	   total++;
		        	   c_withdrawn++;
		           }   
		           else
		           {
		        	   total++;
		           }
		        }
		        
		        double success_rate = ((certified + c_withdrawn)/total)*100;
		       if(success_rate >= 70 && total > 1000)
		        {
		        String employers = key.toString();
		        String combine = employers+"\t"+Long.toString(total);
		        keys.set(combine);
		        context.write(keys, new DoubleWritable(success_rate));
		        }
		        }
		    }

	
	  public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();
		    //conf.set("name", "value")
		    
		    Job job = Job.getInstance(conf, "Count");
		    job.setJarByClass(Q10.class);
		    job.setMapperClass(MapClass.class);
		    //job.setCombinerClass(ReduceClass.class);
		    job.setReducerClass(ReduceClass.class);
		    //job.setNumReduceTasks(0);
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(Text.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(DoubleWritable.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
}
