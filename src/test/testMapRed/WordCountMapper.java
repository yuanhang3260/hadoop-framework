package test.testMapRed;

import global.Hdfs.JobTracker;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import mapreduce.core.Mapper;
import mapreduce.core.OutputCollector;
import mapreduce.io.IntWritable;
import mapreduce.io.LongWritable;
import mapreduce.io.Text;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output) {
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
	
	public static void main(String[] args) {
		JobTracker jt = new JobTracker();
	}

}
