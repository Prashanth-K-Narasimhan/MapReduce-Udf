package mapSideJoin;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.Hashtable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MapSideJoin {

	public static class CustsMapper extends
	Mapper<Object, Text, Text, Text> {
		
		public Text OutputKey = new Text();
		public Text OutputValue = new Text();
		
		public Hashtable<String, String> abMap = new Hashtable<String, String>(100);
		public Hashtable<String, String> abMap1 = new Hashtable<String, String>(100);

		protected void setup(Context context) throws java.io.IOException,
		InterruptedException{

		super.setup(context);
		
		URI[] files = context.getCacheFiles(); // getCacheFiles returns null

		Path p = new Path(files[0]);

		Path p1 = new Path(files[1]);

		if (p.getName().equals("salary.txt")) 
		{
		   BufferedReader reader = new BufferedReader(new FileReader(p.toString()));
		   String line = reader.readLine();
		   while(line != null) 
		   {
		                   String[] tokens = line.split(",");
		                   String emp_id = tokens[0];
		                   String emp_sal = tokens[1];
		                   abMap.put(emp_id, emp_sal);
		                   line = reader.readLine();
		   }
	        reader.close();
		   }
		   if (p1.getName().equals("desig.txt")) 
		   {
			   BufferedReader reader = new BufferedReader(new FileReader(p1.toString()));
		       String line = reader.readLine();
		       while(line != null) 
		       {
		    	   String[] tokens = line.split(",");
		    	   String emp_id = tokens[0];
		    	   String emp_desig = tokens[1];
		    	   abMap1.put(emp_id, emp_desig);
		    	   line = reader.readLine();
		       }
		       reader.close();
		       }

		   if (abMap.isEmpty()) 
		   {
			   throw new IOException("MyError:Unable to load salary data.");
		   }

		   if (abMap1.isEmpty()) 
		   {
			   throw new IOException("MyError:Unable to load designation data.");
		   }  		
	}	
public void map (Object key, Text value, Context context)throws IOException, InterruptedException
{
	String row = value.toString();
	String[] token = row.split(",");
	String emp_id = token[0];
	String salary = abMap.get(emp_id);
	String desig = abMap1.get(emp_id);
	String salary_desig = salary+","+desig;
	OutputKey.set(row);
	OutputValue.set(salary_desig);
	context.write(OutputKey, OutputValue);
}

public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    //conf.set("name", "value")
    
    Job job = Job.getInstance(conf, "Map_Side");
    job.setJarByClass(MapSideJoin.class);
    job.setMapperClass(CustsMapper.class);
    
    job.addCacheFile(new Path("/myapp/salary.txt").toUri());
    job.addCacheFile(new Path("/myapp/desig.txt").toUri());
    
    //job.setCombinerClass(ReduceClass.class);
    //job.setNumReduceTasks(0);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
}

}
}