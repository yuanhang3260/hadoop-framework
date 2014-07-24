package mapreduce.jobtracker;

import java.io.Serializable;
import java.util.Date;
import java.util.List;

/**
 * TaskTracker heatbeat JobTracker periodically by collecting current tasks'
 * status on it, available slots and form a TaskTrackerReport, JobTracker
 * update each task's status according to TaskTracker's report
 *
 */
public class TaskTrackerReport implements Serializable {

	private static final long serialVersionUID = 6483495489789811905L;
	
	public String taskTrackerIp;
	
	public int emptySlot;
	
	public List<TaskStatus> taskStatus;
	
	public TaskTrackerReport(String ip, int numSlot, List<TaskStatus> taskStatus) {
		
		this.taskTrackerIp = ip;
		
		this.emptySlot = numSlot;
		
		this.taskStatus = taskStatus;
		
	}
	
	public void printReport() {
		System.out.format(">>>>>>>>>>>>>>>>>>>>>>%s\n", (new Date()).toString());
		System.out.format("REPORT:\nSlots=%d\n", this.emptySlot);
		int i = 1;
		for (TaskStatus task : this.taskStatus) {
			System.out.format("%d\ttask id=%s\tstatus=%s\n", i, task.taskId, task.status);
			i++;
		}
		System.out.format("<<<<<<<<<<<<<<<<<<<<<<\n\n\n\n", (new Date()).getTime());
	}
}
