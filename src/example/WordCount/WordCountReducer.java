package example.WordCount;

import java.io.IOException;
import java.util.Iterator;

import mapreduce.core.Reducer;
import mapreduce.io.collector.OutputCollector;
import mapreduce.io.writable.IntWritable;
import mapreduce.io.writable.Text;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

	@Override
	public void reduce(Text key, Iterator<IntWritable> values,
			OutputCollector<Text, IntWritable> output) throws IOException {
		int sum = 0;
		while (values.hasNext()) {
			sum += values.next().getValue();
		}
		
		output.collect(key, new IntWritable(sum));
	}
}
