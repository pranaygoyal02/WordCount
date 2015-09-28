package MaxTemp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
//import org.apache.hap.mapred.TextInputFormat;

import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MaxTempDriver  extends Configured implements Tool {
	
	public int run(String[] args) throws Exception {
		//Configuration conf =new Configuration();
		Job job=new Job();
		job.setJarByClass(MaxTempDriver.class);
		job.setInputFormatClass(TextInputFormat.class);
	//job.setOutputFormatClass(TextOutputFormat.class);
	//job.setMapOutputKeyClass(TextOutputFormat.class);
	//job.setMapOutputValueClass(TextOutputFormat.class);
	job.setMapperClass(MaxTempMapper.class);
	job.setReducerClass(MaxTempReducer.class);
	job.setOutputKeyClass(KeyValueTextInputFormat.class);
	job.setOutputValueClass(KeyValueTextInputFormat.class);

	job.setNumReduceTasks(2);
	job.submit();
	return 0;
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stubr
/*		
		Configuration conf =new Configuration();
		Job job=new Job(conf,"MaxTemp");
		job.setJarByClass(MaxTempDriver.class);
		job.setInputFormatClass(TextInputFormat.class);
 //job.setOutputFormatClass(TextOutputFormat.class);
 //job.setMapOutputKeyClass(TextOutputFormat.class);
 //job.setMapOutputValueClass(TextOutputFormat.class);
 job.setMapperClass(MaxTempMapper.class);
 job.setReducerClass(MaxTempReducer.class);
 job.setOutputKeyClass(KeyValueTextInputFormat.class);
 job.setOutputValueClass(KeyValueTextInputFormat.class);
 job.setNumReduceTasks(2);
TextInputFormat.addInputPaths(job, args[0]); 
TextOutputFormat.setOutputPath(job, new Path(args[1]));
System.exit(job.waitForCompletion(true) ? 0 : 1);
*/
		
		Configuration conf = new Configuration();
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    ToolRunner.run(new MaxTempDriver(), otherArgs);
 
	}

}