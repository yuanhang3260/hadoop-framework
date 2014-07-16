package mapreduce.task;

import java.io.Serializable;

import mapreduce.jobtracker.WorkStatus;


public class Task implements Serializable {
	String jobId;
	String tid;

	//InputFormat;
	//WorkStatus status;
	
	public Task(String tid, String jobId) {
		this.tid = tid;
		this.jobId = jobId;
		//this.status = WorkStatus.RUNNING;
	}
	
	public String getTaskId() {
		return this.tid;
	}
	
	public String getJobId() {
		return this.jobId;
	}
	
	
//	public WorkStatus getTaskStatus() {
//		return this.status;
//	}
	
}
