package ques1b;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Q1_b {
	public static class MapClass extends Mapper<LongWritable,Text,Text,Text>
	   {
	      public void map(LongWritable key, Text value, Context context)
	      {	    
	      try{ 
	    		String record = value.toString();
	    		String[] parts = record.split("\\t");

	    		context.write(new Text(parts[4]), new Text(parts[7]));
	    	}
	         catch(Exception e)
	         {

	        	 System.out.println(e.getMessage());
	      
	         }
	      }
	   }
	 public static class ReduceClass extends Reducer<Text, Text,NullWritable,Text> {
		 private TreeMap<Double,String> sorter = new TreeMap<>();
			 Text keys = new Text();
		    public void reduce(Text key, Iterable<Text> values, Context context) 
		      throws IOException, InterruptedException {
		    	
		    	long [] array = new long[6];
		    	
		        for (Text val : values) {
		        	String year = val.toString();
		        	if(year.equalsIgnoreCase("2011"))
		        	{
		        		array[0] += 1; 
		        	}
		        	if(year.equalsIgnoreCase("2012"))
		        	{
		        		array[1] += 1; 
		        	}
		        	if(year.equalsIgnoreCase("2013"))
		        	{
		        		array[2] += 1; 
		        	}
		        	if(year.equalsIgnoreCase("2014"))
		        	{
		        		array[3] += 1; 
		        	}
		        	if(year.equalsIgnoreCase("2015"))
		        	{
		        		array[4] += 1; 
		        	}
		        	if(year.equalsIgnoreCase("2016"))
		        	{
		        		array[5] += 1; 
		        	}
		        	
		         }
		        try{
		        double growth = 0; int n = 0;
		        for (int i=1;i<6;i++)     
		        { 
		        	growth = growth + ((array[i]-array[i-1])*100/array[i-1]); 
		        	n = n+1;
		        } 
		       	double growth_avg = growth/n;
		        String output = key.toString() +"\t"+ String.format("%.2f",growth_avg);
		        sorter.put(growth_avg, output); 
		        }
		        catch(Exception e)
		        {}	  
		        }
		    
		    protected void cleanup(Context context) throws IOException, InterruptedException{
		 	
		    	int count = 0;
		    	for(String val:sorter.descendingMap().values())
		 		{
					if(count < 5)
					{
						context.write(NullWritable.get(),new Text(val));
						count++;
					}	
				}
		  	 }
	 } 	
	  public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();
		    //conf.set("name", "value")
		    
		    Job job = Job.getInstance(conf, "Q1B");
		    job.setJarByClass(Q1_b.class);
		    job.setMapperClass(MapClass.class);
		    //job.setCombinerClass(ReduceClass.class);
		    job.setReducerClass(ReduceClass.class);
		    //job.setNumReduceTasks(0);
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(Text.class);
		    job.setOutputKeyClass(NullWritable.class);
		    job.setOutputValueClass(Text.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
}
