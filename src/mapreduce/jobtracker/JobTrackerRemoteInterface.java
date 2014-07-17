package mapreduce.jobtracker;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

import mapreduce.Job;
import mapreduce.task.Task;

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
	
	/**
	 * Periodically heatBeat and get tasks assignment from job tracker
	 * @param report
	 * @return
	 */
	public JobTrackerACK heartBeat(TaskTrackerReport report) throws RemoteException;
	
	/**
	 * 
	 * @param jobId
	 * @return int The number of incomplete map tasks
	 * @throws RemoteException
	 */
	public int checkMapProgress(String jobId) throws RemoteException;
	
	/**
	 * 
	 * @param jobId
	 * @return int The number of incomplete reduce tasks
	 * @throws RemoteException
	 */
	public int checkReduceProgress(String jobId) throws RemoteException;
}
