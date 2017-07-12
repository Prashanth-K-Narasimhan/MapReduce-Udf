package ques10;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

	public class ReduceClass extends Reducer <Text,Text,NullWritable,Text>{
		private TreeMap<Double,String> sorter = new TreeMap<>();
		public void reduce(Text key,Iterable<Text> value,Context context){
			double total = 0;
			double success = 0;
			for (Text val:value)
			{
				String status = val.toString();
				if(status.equals("CERTIFIED") || status.equals("CERTIFIED-WITHDRAWN")){
					total++;
					success++;
				}
				else total++;
			}
			
			double success_rate = (success/total)*100;
			if (success_rate >= 70 && total >= 1000){
				String output = key.toString() +"\""+ String.format("%.0f",total)+"\""+ String.format("%.2f",success_rate);
				sorter.put(success_rate, output);				
			}
		}
		protected void cleanup(Context context) throws IOException, InterruptedException{
			for(String val : sorter.descendingMap().values())
			{
				context.write(NullWritable.get(),new Text(val));
			}
		}
	}

