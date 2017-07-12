package udfhive;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class dummy  extends UDF{
        public Text evaluate(Text text){
                if(text==null) return null;
                return text;
        }
}