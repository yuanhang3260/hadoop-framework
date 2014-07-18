package mapreduce.jobtracker;

import java.util.concurrent.ConcurrentHashMap;

import mapreduce.task.Task;

public class JobStatus {
	public String jobId;
	public WorkStatus status;
	public ConcurrentHashMap<String, TaskStatus> mapperStatusTbl;
	public ConcurrentHashMap<String, TaskStatus> reducerStatusTbl;
	public int mapTaskTotal;
	public int mapTaskLeft;
	public int reduceTaskTotal;
	public int reduceTaskLeft;
	
	public JobStatus(String jobId, int mapNum, int reduceNum) {
		this.jobId = jobId;
		this.mapTaskTotal = mapNum;
		this.mapTaskLeft = mapNum;
		this.reduceTaskTotal = reduceNum;
		this.reduceTaskLeft = reduceNum;
		this.mapperStatusTbl = new ConcurrentHashMap<String, TaskStatus>();
		this.reducerStatusTbl = new ConcurrentHashMap<String, TaskStatus>();
		this.status = WorkStatus.RUNNING;
	}
}
