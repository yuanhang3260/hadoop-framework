package mapreduce.jobtracker;

import java.io.Serializable;
import java.util.List;

import mapreduce.task.Task;

/**
 * A JobTrackerACK is used as a return value of heartBeat, it assigns new tasks
 * for the heartBeating taskTracker and acknowledges those FAILED and SUCCESS
 * tasks
 *
 */
public class JobTrackerACK implements Serializable{

	private static final long serialVersionUID = -6359644594992260439L;
	public List<Task> newAddedTasks;
	public List<TaskStatus> rcvTasks;
	
	public JobTrackerACK(List<Task> newAddedTasks, List<TaskStatus> rcvTasks) {
		this.newAddedTasks = newAddedTasks;
		this.rcvTasks = rcvTasks;
	}
	
}
