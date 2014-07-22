package mapreduce.jobtracker;

import java.io.Serializable;
import java.util.List;

import mapreduce.message.Task;

/**
 * A JobTrackerACK is used as a return value of heartBeat, it assigns new tasks
 * for the heartBeating taskTracker and acknowledges those FAILED and SUCCESS
 * tasks
 *
 */
public class JobTrackerACK implements Serializable{

	private static final long serialVersionUID = 1004271801317456453L;
	public List<Task> newAddedTasks;
	public List<TaskStatus> rcvTasks;
	
	public JobTrackerACK(List<Task> newAddedTasks, List<TaskStatus> rcvTasks) {
		this.newAddedTasks = newAddedTasks;
		this.rcvTasks = rcvTasks;
	}

}
