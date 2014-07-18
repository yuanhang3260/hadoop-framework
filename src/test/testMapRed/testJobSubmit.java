package test.testMapRed;

import example.WordCountMapper;
import example.WordCountReducer;
import mapreduce.JobClient;
import mapreduce.JobConf;

public class testJobSubmit {
	public static void main(String[] args) {
		JobConf conf = new JobConf();
		
		conf.setJobName("jobSubmitTest");
		
		conf.setInputPath("hello");
		conf.setOutputPath("hello_diubao");
		
		conf.setMapperClass(WordCountMapper.class);
		conf.setReducerClass(WordCountReducer.class);
		
		conf.setNumReduceTasks(1);
		conf.setPriority(0);
		
		String jobId = JobClient.runJob(conf);
	}
}
