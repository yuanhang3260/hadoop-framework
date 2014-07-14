package mapreduce.jobtracker;

import global.Hdfs;
import hdfs.DataStructure.DataNodeEntry;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

import mapreduce.Job;
import mapreduce.core.Split;
import mapreduce.task.Task;
import mapreduce.task.TaskTrackerRemoteInterface;

public class JobTracker implements JobTrackerRemoteInterface {
	
	private static int MAX_NUM_MAP_TASK = 999;
	
	private int taskNaming = 1;
	
	/* host IP, task tracker port */
	private ConcurrentHashMap<String, TaskTrackerInfo> taskTrackerTbl = new ConcurrentHashMap<String, TaskTrackerInfo>();
	
	/* keep jobs in this tbl after submission from jobclient*/
	private ConcurrentHashMap<String, Job> jobTbl = new ConcurrentHashMap<String, Job>();
	
	/* keep all tasks wait to be submit to taskTracker*/
	private ArrayBlockingQueue<Task> taskQueue = new ArrayBlockingQueue<Task>(MAX_NUM_MAP_TASK);
	
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
	public synchronized String join(String ip, int port, int mapSlots, int reduceSlots) {
		String taskTrackerName = ip + ":" + port;
		if (!taskTrackerTbl.containsKey(ip)) {
			TaskTrackerInfo stat = new TaskTrackerInfo(ip, port, mapSlots, reduceSlots);
			taskTrackerTbl.put(ip, stat);
		}
		//TODO: upon a tasktracker recover from failure, what about those tasks assigned on it?
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
	
	private Task createMapTask(String jobId, Split split, Class<?> theClass, int partitionNum) {
		Task task = new Task(nameTask(), jobId, split, theClass, partitionNum);
		return task;
	}
	
	private synchronized String nameTask() {
		int taskName = taskNaming++;
		return Integer.toString(taskName);
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
			Task task = createMapTask(job.getJobId(), split, job.getJobConf().getMapper(), job.getJobConf().getNumReduceTasks());
			this.taskQueue.add(task);
		}
	}
	
	
	/* a FIFO JobScheduler */
	private class JobScheduler implements Runnable{

		@Override
		public void run() {
			// TODO Auto-generated method stub
			while (true) {
				Task nextTask = taskQueue.peek();
				
				if (nextTask instanceof Task) {
					/* schedule a map task */
					TaskTrackerInfo taskTracker = pickTaskTrackerForMap(nextTask);
					if (taskTracker != null) {
						submitTask(taskQueue.poll(), taskTracker.getIp(), taskTracker.getPort());
					}
				} else {
					/* schedule a reduce task */
				}
			}
		}
		
		public synchronized TaskTrackerInfo pickTaskTrackerForMap(Task task) {
			int chunkIdx = task.getSplit().getChunkIdx();
			List<DataNodeEntry> nodes = task.getSplit().getFile().getChunkList().get(chunkIdx).getAllLocations();
			for (DataNodeEntry node : nodes) {
				String ip = node.dataNodeRegistryIP;
				if (taskTrackerTbl.containsKey(ip)) {
					TaskTrackerInfo taskTrackerInfo = taskTrackerTbl.get(ip);
					if (taskTrackerInfo.getStatus() == TaskTrackerInfo.Status.RUNNING && 
							taskTrackerInfo.getMapSlot() > 0) {
						return taskTrackerInfo;
					}
				}
			}
			
			/* all data nodes with that split locally is busy now, pick a nearest one */
			Set<String> allNodes = taskTrackerTbl.keySet();
			for (String ip : allNodes) {
				
			}
			return null;
		}
	}
	
}
