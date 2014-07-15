package mapreduce.task;

import java.io.Serializable;


public class Task implements Serializable {
	String jobId;
	String tid;

	//InputFormat;
	TaskStatus status;
	
	private enum TaskStatus {
		RUNNING, TERMINATED, FAILED;
	}
	
	public Task(String tid, String jobId) {
		this.tid = tid;
		this.jobId = jobId;
		this.status = TaskStatus.RUNNING;
	}
	
	public String getTaskId() {
		return this.tid;
	}
	
	public String getJobId() {
		return this.jobId;
	}
	
	
	public TaskStatus getTaskStatus() {
		return this.status;
	}
	
}
