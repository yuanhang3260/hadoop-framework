package mapreduce.jobtracker;

import java.io.Serializable;
import java.util.List;

import mapreduce.task.Task;

public class JobTrackerACK implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1004271801317456453L;
	public List<Task> newAddedTasks;
	public List<TaskStatus> rcvTasks;
	
}
