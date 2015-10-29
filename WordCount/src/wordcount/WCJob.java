package wordcount;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class WCJob {
	public static void  main(String [] args) throws IOException, ClassNotFoundException, InterruptedException {
		// Creating a new job
		Job job = new Job();
		job.setJobName("Count word");
		job.setJarByClass(WCJob.class);
		
		// Set input output paths
		FileInputFormat.addInputPath(job, new Path("InFile"));	
		FileOutputFormat.setOutputPath(job, new Path("OutFile"));
		
		// Set mapper, reducer and output data types
		job.setReducerClass(WCReduce.class);
		job.setMapperClass(WCMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		
		// Wait for the job to complete
		System.exit(job.waitForCompletion(true)? 0:1);
		
	}

}
