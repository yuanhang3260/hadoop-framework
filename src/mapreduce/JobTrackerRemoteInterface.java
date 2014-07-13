package mapreduce;

import java.rmi.Remote;

public interface JobTrackerRemoteInterface extends Remote {
	/**
	 * Submit a job for JobTracker to run (from client)
	 * @param job
	 * @return Id of the job assigned by JobTracker
	 */
	public String submitJob(Job job);
}
