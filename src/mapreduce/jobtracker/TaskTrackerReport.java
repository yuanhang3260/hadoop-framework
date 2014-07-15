package mapreduce.jobtracker;

import java.util.List;

import mapreduce.task.Task;

public class TaskTrackerReport {
	public int emptyMapSlot;
	public int emptyReduceSlot;
	public List<Task> tasks;
	
	public TaskTrackerReport(int mapSlot, int reduceSlot, List<Task> taskStatus) {
		this.emptyMapSlot = mapSlot;
		this.emptyReduceSlot = reduceSlot;
		this.tasks = taskStatus;
	}
}
