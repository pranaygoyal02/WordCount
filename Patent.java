


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
//import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.mockito.asm.tree.analysis.Value;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.StringTokenizer;


    
    
    public class Patent
    {
    //Mapper class	
    	public static class PatentCountMapper extends Mapper<Text,Text,Text,Text>
        
        	{
    			//private final static IntWritable one = new IntWritable(1);
				private Text key1 = new Text();
				private Text value1=new Text();
				public void map(Text key,Text value,Context context) throws IOException, InterruptedException
                    {
					System.out.println("value of key" + key.toString()+"Value of second "+ value.toString());
				/*	StringTokenizer itr = new StringTokenizer(key.toString()," ");
						while (itr.hasMoreTokens()) {
														key1.set(itr.nextToken());
														//System.out.println("The value of key1 is :" + itr.nextToken());
														//value1.set(itr.nextToken());
														//System.out.println("The value of value is :" + itr.nextToken());
											            context.write(key1,value1);//Intermediate Key value pairs
													}	
													*/
							
                    }
        	}
    
    	
    //Reducer Class
		public static class IntSumReducer extends Reducer<Text,Text,Text,IntWritable> 
		{
				
					private IntWritable result = new IntWritable();

				public void reduce(Text key, Iterable<Text> value, Context context ) throws IOException,InterruptedException 
				{
					
					for (Text val : value)
						{
			               //do something
							int sum=0;
				 
								{
									sum++;
									//System.out.println("Value of sum is :"+sum);
									//System.out.println("The value of sum when the value of key  is : "+key +"is" + sum);
								}
							result.set(sum);
							context.write(key,result);
						}
				}
		}
				
		public static void main(String[] args) throws Exception

		{
			Configuration conf = new Configuration();
			//conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.seperator"," ");
            conf.set("key.value.separator.in.input.line",",");
			String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
			if (otherArgs.length != 2) 
				{
					System.err.println("Usage: patent <in> <out>");
					System.exit(2);
				}
			Job job = new Job(conf, "Patent");
			job.setJarByClass(Patent.class);
			job.setMapperClass(PatentCountMapper.class);
			job.setInputFormatClass(KeyValueTextInputFormat.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			KeyValueTextInputFormat.addInputPath(job, new Path(otherArgs[0]));
			job.setReducerClass(IntSumReducer.class);
			FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
    }

    
				
				
    
    	
   