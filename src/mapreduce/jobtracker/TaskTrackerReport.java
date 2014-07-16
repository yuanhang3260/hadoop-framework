package mapreduce.jobtracker;

import java.io.Serializable;
import java.util.List;

import mapreduce.task.Task;

public class TaskTrackerReport implements Serializable {
	public String taskTrackerIp;
	public int emptySlot;
	public List<TaskStatus> taskStatus;
	
	public TaskTrackerReport(String ip, int numSlot, List<TaskStatus> taskStatus) {
		this.taskTrackerIp = ip;
		this.emptySlot = numSlot;
		this.taskStatus = taskStatus;
	}
}
