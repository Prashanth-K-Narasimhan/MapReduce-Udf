package day2MapSideJoin;

import java.io.BufferedReader;

import java.io.FileReader;

import java.io.IOException;

import java.util.HashMap;


import org.apache.hadoop.filecache.DistributedCache;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.DoubleWritable;

import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;


@SuppressWarnings("deprecation")
public class myMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	
HashMap<String, String> hm= new HashMap<>();
	
public void setup(Context c) throws IOException{
		
Path[] allFiles = DistributedCache.getLocalCacheFiles(c.getConfiguration());		
		
for(Path eachFile : allFiles){
			
if(eachFile.getName().equals("file1")){
				
FileReader fr = new FileReader(eachFile.toString());
				
BufferedReader br = new BufferedReader(fr);
				
String line =br.readLine();
				
while(line != null){
					
String[] eachVal = line.split(" ");
					
String id = eachVal[0];
					
String name = eachVal[1];
					
hm.put(id, name);
					
line=br.readLine();
				
}
				
br.close();
			
}
			
if (hm.isEmpty()) 
			
{
				
throw new IOException("Unable To Load file1");
			
}
		
}
	
}
	
	
public void map(LongWritable mInpKey, Text mInpVal, Context c) throws IOException, InterruptedException{
		
String line = mInpVal.toString();
		
String eachVal[] = line.split(" ");
		
String id=eachVal[0];
		
String amt= eachVal[1];
		
String name = hm.get(id);
Text mOutKey = new Text("User ID : " +id +" Name : " +name);
DoubleWritable mOutVal = new DoubleWritable(Double.parseDouble(amt));
c.write(mOutKey, mOutVal);
} 	
}