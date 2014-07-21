package test.testMapRed;

import mapreduce.JobClient;
import mapreduce.JobConf;
import example.WordCount.WordCountMapper;
import example.WordCount.WordCountReducer;

public class testJobSubmit_WordCount {
	public static void main(String[] args) {
		JobConf conf = new JobConf();
		
		conf.setJobName("jobSubmitTest");
		
		conf.setInputPath("hello");
		conf.setOutputPath("wordcount");
		
		conf.setMapperClassName("example.WordCount.WordCountMapper");
		conf.setReducerClassName("example.WordCount.WordCountReducer");
		
		conf.setNumReduceTasks(2);
		conf.setPriority(0);
		
		//conf.setJarFileEntry("localhost", port, path);
		
		String jobId = JobClient.runJob(conf);
	}
}
