package distriCache;

import java.io.IOException;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

@SuppressWarnings("deprecation")
public class NcdcMaxTempReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
	private NcdcStationMetadataParcer metadata;
	String fileName ;
	String stationName;
	
	protected void setup(Context context) throws IOException
	{
		try{
		Path[] localFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		for (Path eachPath : localFiles) {
		fileName = eachPath.getName().toString().trim();
			if (fileName.equals("station.txt")) 
			{
				metadata = new NcdcStationMetadataParcer();
				metadata.initialize(eachPath);
				break;
			}
		}
		System.out.println("File : "+ localFiles[0].toString());
		}
		catch(NullPointerException e)
		{
			System.out.println("Exception : "+e);
		}
		
		System.out.println("Cache : "+context.getConfiguration().get("mapred.cache.files"));
	}
	
	protected void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException
	{
		
		stationName = metadata.getStationName(key.toString());
		int maxValue = Integer.MIN_VALUE;
		for (IntWritable val:values)
		{
			maxValue = Math.max(maxValue, val.get());
		}
		context.write(new Text(stationName),new IntWritable(maxValue));
	}
}
