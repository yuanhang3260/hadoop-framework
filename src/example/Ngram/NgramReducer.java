package example.Ngram;

import java.io.IOException;
import java.util.Iterator;

import mapreduce.core.Reducer;
import mapreduce.io.collector.OutputCollector;
import mapreduce.io.writable.IntWritable;
import mapreduce.io.writable.Text;

public class NgramReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

	@Override
	public void reduce(Text key, Iterator<IntWritable> values,
			OutputCollector<Text, IntWritable> output) throws IOException {
		int sum = 0;
		while (values.hasNext()) {
			sum++;
			values.next();
		}
		
		output.collect(key, new IntWritable(sum));
	}
}
