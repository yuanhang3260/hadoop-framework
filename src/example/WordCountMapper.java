package example;

import mapreduce.core.Mapper;
import mapreduce.io.collector.OutputCollector;
import mapreduce.io.writable.IntWritable;
import mapreduce.io.writable.Text;

public class WordCountMapper extends Mapper<Text, Text, Text, IntWritable> {

	@Override
	public void map(Text key, Text value, OutputCollector<Text, IntWritable> output) {
		String[] words = value.toString().split(" ");
		for (String word : words) {
			output.collect(new Text(word), new IntWritable(1));
		}
	}

}
