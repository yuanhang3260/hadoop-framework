package mapreduce.jobtracker;

import java.util.Comparator;

import mapreduce.task.Task;

/**
 * This comparator determines the priority of a task when it is added to 
 * task schedule table
 * For now: a. higher priority goes first; b. task with earlier jobId goes
 * first; c. earlier taskId goes first
 * 
 */
public class SchedulerComparator implements Comparator<Task>{

	@Override
	public int compare(Task t1, Task t2) {
		if (t1.getPriority() != t2.getPriority()) {
			/* higher priority goes first */
			return t2.getPriority() - t1.getPriority();
		}
		long time1 = Long.parseLong(t1.getJobId());
		long time2 = Long.parseLong(t2.getJobId());
		if (time1 > time2) {
			return 1;
		} else if (time1 < time2) {
			return -1;
		} else {
			return t1.getTaskId().compareTo(t2.getTaskId());
		}
	}

}
