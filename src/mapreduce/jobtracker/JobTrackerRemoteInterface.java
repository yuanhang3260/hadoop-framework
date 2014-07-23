package mapreduce.jobtracker;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.AbstractMap;
import java.util.Queue;

import mapreduce.message.Job;
import mapreduce.message.Task;

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
	public String join(String ip, int port, int serverPort, int numSlots) throws RemoteException;
	
	/**
	 * Periodically heatBeat and get tasks assignment from job tracker
	 * 	
	 * Each Task Tracker periodically calls this method to send a report to
	 * Job Tracker. The report includes the status of all tasks currently on
	 * the specific Task Tracker, the Job Tracker updates associative 
	 * information and sends acknowledgement of SUCCESS / FAILED tasks back to
	 * Task Tracker (so they will not be in the report next time), along with 
	 * some new task assignments, if there are available slots
	 * 
	 * @param report
	 * @return
	 */
	public JobTrackerACK heartBeat(TaskTrackerReport report) throws RemoteException;
	
	/**
	 * @deprecated
	 * @param jobId
	 * @return int The number of incomplete map tasks
	 * @throws RemoteException
	 */
	public int checkMapProgress(String jobId) throws RemoteException;
	
	/**
	 * @deprecated
	 * @param jobId
	 * @return int The number of incomplete reduce tasks
	 * @throws RemoteException
	 */
	public int checkReduceProgress(String jobId) throws RemoteException;
	
	/**
	 * Client get a job's current progress by this function
	 * @param jobId
	 * @return JobStatus current status of the job
	 * @throws RemoteException
	 */
	public JobStatus getJobStatus(String jobId) throws RemoteException;
	
	public AbstractMap<String, JobStatus> getAllJobStatus() throws RemoteException;
	
	/**
	 * terminate all the tasks of this Job
	 * @param jobId
	 * @throws RemoteException
	 */
	public void terminateJob(String jobId) throws RemoteException;
	
	public AbstractMap<String, TaskTrackerInfo> getTaskTrackerStatus() throws RemoteException;
	
	public AbstractMap<String, Queue<Task>> getScheduleTbl() throws RemoteException;
}
