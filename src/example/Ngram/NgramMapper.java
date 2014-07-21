package example.Ngram;

import mapreduce.core.Mapper;
import mapreduce.io.collector.OutputCollector;
import mapreduce.io.writable.IntWritable;
import mapreduce.io.writable.Text;

public class NgramMapper extends Mapper<Text, Text, Text, IntWritable>{
	
	public static int nGram = 3;
	@Override
	public void map(Text key, Text value,
			OutputCollector<Text, IntWritable> output) {
		String line = value.toString();
		String[] words = Tokenizer.tokenize(line);
		int numWords = words.length;
		StringBuilder sb;
		for (int i = 0; i <= numWords - nGram; i++) {
			sb = new StringBuilder();
			for (int j = 0; j < nGram; j++) {
				sb.append(words[i + j]);
				if (j != nGram - 1) {
					sb.append(" ");
				}
			}
			output.collect(new Text(sb.toString()), new IntWritable(1));
		}
	}

}
