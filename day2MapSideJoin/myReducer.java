package day2MapSideJoin;

import java.io.IOException;


import org.apache.hadoop.io.DoubleWritable;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;


public class myReducer extends Reducer<Text, DoubleWritable, Text, Text> {
	
public void reduce(Text rInpKey, Iterable<DoubleWritable> rInpVal,Context c ) throws IOException, InterruptedException{
		
double amt=0.0;
		
for(DoubleWritable each: rInpVal){
			
amt+= Double.parseDouble(each.toString());
		
}
		
c.write(rInpKey, new Text(" Amount : " +amt));
	
}


}
