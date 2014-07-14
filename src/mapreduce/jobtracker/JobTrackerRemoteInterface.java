package mapreduce.jobtracker;

import java.rmi.Remote;
import java.rmi.RemoteException;

import mapreduce.Job;

public interface JobTrackerRemoteInterface extends Remote {
	/**
	 * Submit a job for JobTracker to run (from client)
	 * @param job
	 * @return Id of the job assigned by JobTracker
	 */
	public String submitJob(Job job) throws RemoteException;
	
	/**
	 * Upon boot, a Task Tracker calls this method to join the cluster by 
	 * registering on Job Tracker
	 * @param ip
	 * @param port
	 * @return String the name of the Task Tracker upon success, the name is 
	 * 				  currently formed by ip : port
	 */
	public String join(String ip, int port, int mapSlots, int reduceSlots) throws RemoteException;
}
