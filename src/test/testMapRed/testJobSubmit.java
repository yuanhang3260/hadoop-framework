package test.testMapRed;

import example.WordCountMapper;
import example.WordCountReducer;
import mapreduce.JobClient;
import mapreduce.JobConf;

public class testJobSubmit {
	public static void main(String[] args) {
		JobConf conf = new JobConf();
		conf.setInputPath("hello");
		conf.setOutputPath("result");
		conf.setJobName("jobSubmitTest");
		conf.setMapperClass(WordCountMapper.class);
		conf.setReducerClass(null);
		conf.setNumReduceTasks(2);
		conf.setPriority(0);
		conf.setReducerClass(WordCountReducer.class);
		conf.setOutputPath("result");
		
		String jobId = JobClient.runJob(conf);
		System.out.println("Job ID: " + jobId);
		System.out.println("DEBUG testJobSubmit(): numReduceTasks = " + conf.getNumReduceTasks());
	}
}
