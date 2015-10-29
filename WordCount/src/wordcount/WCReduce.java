package wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class WCReduce extends Reducer<Text, IntWritable, Text, IntWritable>{

	@Override
	public void reduce(Text key, Iterable<IntWritable> allCounts, Context ctx) throws IOException, InterruptedException{
		int totalSum = 0;
		for(IntWritable count : allCounts)
			totalSum += count.get();
		ctx.write(key, new IntWritable(totalSum));
	}
}
