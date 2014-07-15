package mapreduce.jobtracker;

import java.util.concurrent.ConcurrentHashMap;

import mapreduce.task.Task;

public class JobStatus {
	public Status status;
	public ConcurrentHashMap<String, Task> taskStatusTbl;
	public int mapTaskTotal;
	public int mapTaskLeft;
	public int reduceTaskTotal;
	public int reduceTaskLeft;
	
	public JobStatus(int mapNum, int reduceNum) {
		this.mapTaskTotal = mapNum;
		this.mapTaskLeft = mapNum;
		this.reduceTaskTotal = reduceNum;
		this.reduceTaskLeft = reduceNum;
		this.taskStatusTbl = new ConcurrentHashMap<String, Task>();
		this.status = Status.RUNNING;
	}
	
	public enum Status {
		RUNNING, FAILED, TERMINATED;
	}
}
