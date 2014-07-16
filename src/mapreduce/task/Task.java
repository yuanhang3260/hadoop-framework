package mapreduce.task;

import java.io.Serializable;

import mapreduce.jobtracker.WorkStatus;


public class Task implements Serializable {
	String jobId;
	String tid;
	int priorityLevel;
	/* indicate the number of times that this task has been re-scheduled after failure */
	int rescheduleNum;

	//InputFormat;
	//WorkStatus status;
	
	public Task(String tid, String jobId, int level) {
		this.tid = tid;
		this.jobId = jobId;
		this.priorityLevel = level;
		//this.status = WorkStatus.RUNNING;
	}
	
	public int getRescheduleNum() {
		return this.rescheduleNum;
	}
	
	public void increaseRescheuleNum() {
		this.rescheduleNum++;
	}
	
	public void increasePriority() {
		this.priorityLevel++;
	}
	
	public int getPriority() {
		return this.priorityLevel;
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
