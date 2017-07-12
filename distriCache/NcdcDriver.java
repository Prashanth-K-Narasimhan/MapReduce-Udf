package distriCache;


import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


	@SuppressWarnings("deprecation")
	public class NcdcDriver {

		public static void main(String[] arg) throws IOException, ClassNotFoundException, InterruptedException
		{
			Configuration conf = new Configuration();
			Job job = new Job(conf,"Distributed Cache");
			
			job.setJarByClass(NcdcDriver.class);
			
			FileInputFormat.addInputPath(job, new Path(arg[0]));
			FileOutputFormat.setOutputPath(job, new Path(arg[1]));
//			DistributedCache.addCacheFile(new URI("hdfs://localhost:8020/MR-INPUT/station.txt"), job.getConfiguration());
			DistributedCache.addCacheFile(new Path("/MR-INPUT/station.txt").toUri(), job.getConfiguration());			
			job.setMapperClass(NcdcMaxTempMapper.class);
			job.setCombinerClass(NcdcMaxTempCombiner.class);
			job.setReducerClass(NcdcMaxTempReducer.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
}
