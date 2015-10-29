package wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

// Mapper <input key, input value, output key, output value>
public class WCMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	// For storing individual words as key to the output
	private Text word = new Text();
	// For storing individual count which is 1 for the first time
	private static final IntWritable count = new IntWritable(1);
	
	// Overrriding the map method
	@Override
	public void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException{
		StringTokenizer line = new StringTokenizer(value.toString());
		while(line.hasMoreTokens()){
			word.set(line.nextToken());
			ctx.write(word, count);
		}
	}
	
	

}
