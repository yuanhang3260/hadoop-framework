package test.testMapRed;

import mapreduce.JobClient;
import mapreduce.JobConf;
import example.Ngram.NgramMapper;
import example.Ngram.NgramReducer;

public class testJobSubmit_Ngram {
	public static void main(String[] args) {
		JobConf conf = new JobConf();
		
		conf.setJobName("nGram2");
		
		conf.setInputPath("hello");
		conf.setOutputPath("ngram2");
		
//		conf.setMapperClass(WordCountMapper.class);
//		conf.setReducerClass(WordCountReducer.class);
		
		conf.setMapperClass(NgramMapper.class);
		conf.setReducerClass(NgramReducer.class);
		
		conf.setNumReduceTasks(2);
		conf.setPriority(0);
		
		String jobId = JobClient.runJob(conf);
	}
}
