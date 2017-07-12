package hivecount;

import java.util.StringTokenizer;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class Count extends UDF{
	private Text word = new Text();
	
	public Text evaluate(Text text){	
		String line = text.toString();
		StringTokenizer tokenizer = new StringTokenizer(line);
		while (tokenizer.hasMoreTokens()) {
        word.set(tokenizer.nextToken());
		}
		return new Text(word);
		}
		}