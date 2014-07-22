package test.testMapRed;

import mapreduce.client.JobClient;
import mapreduce.client.JobConf;

public class testJobSubmit_Ngram {
	public static void main(String[] args) {
		JobConf conf = new JobConf();
		
		conf.setJobName("nGram2");
		
		conf.setInputPath("hello");
		conf.setOutputPath("ngram2");
		

		conf.setMapperClassName("example.Ngram.NgramMapper");
		conf.setReducerClassName("example.Ngram.NgramReducer");
		
		conf.setNumReduceTasks(2);
		conf.setPriority(0);
		
		String jobId = JobClient.runJob(conf);
	}
}
