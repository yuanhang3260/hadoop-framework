package mapreduce.jobtracker;

import global.Hdfs;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Date;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import mapreduce.Job;
import mapreduce.core.Split;
import mapreduce.task.Task;
import mapreduce.task.TaskTrackerRemoteInterface;

public class JobTracker implements JobTrackerRemoteInterface {
	
	/* host IP, task tracker port */
	private ConcurrentHashMap<String, Integer> taskTrackerTbl = new ConcurrentHashMap<String, Integer>();
	
	/* keep jobs in this tbl after submission from jobclient*/
	private ConcurrentHashMap<String, Job> jobTbl = new ConcurrentHashMap<String, Job>();
	
	/* keep all tasks wait to be submit to taskTracker*/
	private ArrayBlockingQueue<Task> mapQueue = new ArrayBlockingQueue<Task>();
	
//	public JobTracker() {
//		this.taskTrackerTbl = new ConcurrentHashMap<String, Integer>();
//		this.mapTaskQueue = new ConcurrentLinkedQueue<Task>();
//	}
	
	public void init() {
		try {
			Registry jtRegistry = LocateRegistry.createRegistry(Hdfs.JobTracker.jobTrackerRegistryPort);
			JobTrackerRemoteInterface jtStub = (JobTrackerRemoteInterface) UnicastRemoteObject.exportObject(this, 0);
			jtRegistry.rebind(Hdfs.JobTracker.jobTrackerServiceName, jtStub);
		} catch (RemoteException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public synchronized String join(String ip, int port) {
		String taskTrackerName = ip + ":" + port;
		if (!taskTrackerTbl.containsKey(ip)) {
			taskTrackerTbl.put(ip, port);
		}
		return taskTrackerName;
	}
	
	/**
	 * call a specific task tracker to sumbit a task to run
	 * 
	 * @param task
	 * @param taskTrackerIp
	 * @param taskTrackerPort
	 */
	public void submitTask(Task task, String taskTrackerIp, int taskTrackerPort) {
		Registry taskTrackerRegistry;
		try {
			taskTrackerRegistry = LocateRegistry.getRegistry(taskTrackerIp, taskTrackerPort);
			TaskTrackerRemoteInterface taskTrackerStub = (TaskTrackerRemoteInterface) taskTrackerRegistry.lookup(Hdfs.TaskTracker.taskTrackerServiceName);
			boolean ret = taskTrackerStub.runTask(task);
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (NotBoundException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * JobClient calls this method to submit a job to schedule
	 */
	@Override
	public synchronized String submitJob(Job job) {
		String jobId = String.format("%d", new Date().getTime());
		jobTbl.put(jobId, job);
		addMapTasks(job);
		return jobId;
	}
	
	private void addMapTasks(Job job) {
		for (Split split : job.getSplit()) {
			Task task = new Task(job.getJobId(), split, job.getJobConf().getMapper(), job.getJobConf().getNumReduceTasks());
			this.mapTaskQueue.add(task);
		}
	}
	
	
	/* a FIFO JobScheduler */
	private class JobScheduler {
		public Queue<Task> taskQueue;
	}
	
}
