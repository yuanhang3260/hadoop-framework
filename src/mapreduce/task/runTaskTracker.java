package mapreduce.task;

import global.MapReduce;

public class runTaskTracker {
	public static void main(String[] args) {
		TaskTracker tt = 
				new TaskTracker(MapReduce.JobTracker.jobTrackerRegistryIp, MapReduce.JobTracker.jobTrackerRegistryPort, MapReduce.TasktTracker1.taskTrackerPort);
		
	}
}
