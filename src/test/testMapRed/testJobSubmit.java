package test.testMapRed;

import mapreduce.JobClient;
import mapreduce.JobConf;
import example.Ngram.NgramMapper;
import example.Ngram.NgramReducer;

public class testJobSubmit {
	public static void main(String[] args) {
		JobConf conf = new JobConf();
		
		conf.setJobName("jobSubmitTest");
		
		conf.setInputPath("hello");
		conf.setOutputPath("hello");
		
//		conf.setMapperClass(WordCountMapper.class);
//		conf.setReducerClass(WordCountReducer.class);
		
		conf.setMapperClass(NgramMapper.class);
		conf.setReducerClass(NgramReducer.class);
		
		conf.setNumReduceTasks(1);
		conf.setPriority(0);
		
		String jobId = JobClient.runJob(conf);
	}
}
