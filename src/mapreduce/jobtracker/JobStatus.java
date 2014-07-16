package mapreduce.jobtracker;

import java.util.concurrent.ConcurrentHashMap;

import mapreduce.task.Task;

public class JobStatus {
	//public String jobId;
	public WorkStatus status;
	public ConcurrentHashMap<String, TaskStatus> taskStatusTbl;
	public int mapTaskTotal;
	public int mapTaskLeft;
	public int reduceTaskTotal;
	public int reduceTaskLeft;
	
	public JobStatus(int mapNum, int reduceNum) {
		this.mapTaskTotal = mapNum;
		this.mapTaskLeft = mapNum;
		this.reduceTaskTotal = reduceNum;
		this.reduceTaskLeft = reduceNum;
		this.taskStatusTbl = new ConcurrentHashMap<String, TaskStatus>();
		this.status = WorkStatus.RUNNING;
	}
}
