package mapreduce.jobtracker;

import global.Hdfs;

public class runJobTracker {
	public static void main(String[] args) {
		JobTracker jt = new JobTracker();
		jt.init();
		if (Hdfs.DEBUG) {
			System.out.println("DEBUG runJobTracker.main(): jobTracker now running");
		}
	}
}
