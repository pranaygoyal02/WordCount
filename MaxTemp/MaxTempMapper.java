package MaxTemp;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxTempMapper extends Mapper<Text,Text,Text,Text> {
	private IntWritable one=new IntWritable();
	
	protected void map(Text key1,Text value1,Context context) throws IOException,InterruptedException 
	{
	  
		/*String s1=value.toString();
	  StringTokenizer st=new StringTokenizer(s1," ");
	  while (st.hasMoreTokens())
	  {
		  key=st.nextToken();
		  value=st.nextToken();
	  }
	  */
		context.write(key1,value1);
		System.out.println("value of key :" +key1+ "value of value1"+value1);
		
	}

}
