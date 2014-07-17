package mapreduce.jobtracker;

import java.util.List;

import mapreduce.task.Task;

public class JobTrackerACK {
	public List<Task> newAddedTasks;
	public List<TaskStatus> rcvTasks;
	
}
