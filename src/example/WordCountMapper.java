package example;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import mapreduce.core.Mapper;
import mapreduce.io.collector.OutputCollector;
import mapreduce.io.writable.IntWritable;
import mapreduce.io.writable.Text;

public class WordCountMapper extends Mapper<Text, Text, Text, IntWritable> {

	@Override
	public void map(Text key, Text value, OutputCollector<Text, IntWritable> output) {
		Map<String, Integer> wordCount = new HashMap<String, Integer>();
		String line = value.toString();
		String[] words = line.split(" ");
		for (String word : words) {
			if (wordCount.containsKey(word)) {
				wordCount.put(word, wordCount.get(word) + 1);
			} else {
				wordCount.put(word, 1);
			}
		}
		
		Set<String> keySet = wordCount.keySet();
		for (String word : keySet) {
			output.collect(new Text(word), new IntWritable(wordCount.get(word)));
		}
		
	}

}
