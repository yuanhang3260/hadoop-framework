package mapreduce.jobtracker;

import global.Hdfs;
import global.MapReduce;
import hdfs.DataStructure.DataNodeEntry;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import mapreduce.Job;
import mapreduce.io.Split;
import mapreduce.task.MapperTask;
import mapreduce.task.Task;
import mapreduce.tasktracker.TaskTrackerRemoteInterface;

public class JobTracker implements JobTrackerRemoteInterface {
	
	private static int MAX_NUM_MAP_TASK = 999;
	
	private int taskNaming = 1;
	
	private JobScheduler jobScheduler;
	
	/* host IP, task tracker port */
	private ConcurrentHashMap<String, TaskTrackerInfo> taskTrackerTbl = new ConcurrentHashMap<String, TaskTrackerInfo>();
	
	/* keep jobs in this tbl after submission from jobclient*/
	private ConcurrentHashMap<String, Job> jobTbl = new ConcurrentHashMap<String, Job>();
	
	private ConcurrentHashMap<String, JobStatus> jobStatusTbl = new ConcurrentHashMap<String, JobStatus>();
	
	/* keep all tasks wait to be submit to taskTracker*/
	//private ArrayBlockingQueue<Task> taskQueue = new ArrayBlockingQueue<Task>(MAX_NUM_MAP_TASK);
	
	public void init() {
//		Thread t = new Thread(new JobScheduler());
//		t.start();
		jobScheduler = new JobScheduler();
		try {
			Registry jtRegistry = LocateRegistry.createRegistry(MapReduce.JobTracker.jobTrackerRegistryPort);
			JobTrackerRemoteInterface jtStub = (JobTrackerRemoteInterface) UnicastRemoteObject.exportObject(this, 0);
			jtRegistry.rebind(MapReduce.JobTracker.jobTrackerServiceName, jtStub);
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
			this.jobScheduler.taskTrackerToTasksTbl.put(ip, new ArrayList<Task>());
		}
		if (Hdfs.DEBUG) {
			System.out.println("DEBUG JobTracker.join(): TaskTracker " + taskTrackerName + " join cluster");
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
			TaskTrackerRemoteInterface taskTrackerStub = (TaskTrackerRemoteInterface) taskTrackerRegistry.lookup(MapReduce.TaskTracker.taskTrackerServiceName);
//			boolean ret = taskTrackerStub.runTask(task);
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (NotBoundException e) {
			e.printStackTrace();
		}
	}
	
	private MapperTask createMapTask(String jobId, Split split, Class<?> theClass, int partitionNum) {
		MapperTask task = new MapperTask(nameTask(), jobId, split, theClass, partitionNum);
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
		
		JobStatus jobStatus = new JobStatus(job.getSplit().size(), job.getJobConf().getNumReduceTasks());
		this.jobStatusTbl.put(jobId, jobStatus);
		addMapTasks(job);
		return jobId;
	}
	
	private synchronized void addMapTasks(Job job) {
		for (Split split : job.getSplit()) {
			MapperTask task = createMapTask(job.getJobId(), split, job.getJobConf().getMapper(), job.getJobConf().getNumReduceTasks());
			if (Hdfs.DEBUG) {
				System.out.println("DEBUG JobTracker.addMapTasks(): now adding task " + task.getTaskId() + " to Task Queue");
			}
			
			this.jobScheduler.addMapTask(task);
			this.jobStatusTbl.get(job.getJobId()).taskStatusTbl.put(task.getTaskId(), task);
		}
	}
	
	@Override
	public List<Task> heartBeat(TaskTrackerReport report) {
		// TODO Auto-generated method stub
		return null;
	}
	
	
	/* a FIFO JobScheduler */
	private class JobScheduler {
		
		/* assign tasks to each ip, locality first */
		public ConcurrentHashMap<String, List<Task>> taskTrackerToTasksTbl = new ConcurrentHashMap<String, List<Task>>();
		
		/**
		 * Schedule a task with locality first
		 * @param task
		 */
		public synchronized void addMapTask(MapperTask task) {
			int chunkIdx = task.getSplit().getChunkIdx();
			List<DataNodeEntry> entries = task.getSplit().getFile().getChunkList().get(chunkIdx).getAllLocations();
			/* pick the one among all entries with lightest work-load */
			String bestIp = null;
			int minLoad = Integer.MAX_VALUE;
			for (DataNodeEntry entry : entries) {
				int workLoad = taskTrackerToTasksTbl.get(entry.dataNodeRegistryIP).size();
				if (workLoad < minLoad && workLoad + 1 <= MapReduce.TaskTracker.MAX_NUM_MAP_TASK 
						&& taskTrackerTbl.get(entry.dataNodeRegistryIP).getStatus() == TaskTrackerInfo.Status.RUNNING) {
					bestIp = entry.dataNodeRegistryIP;
					minLoad = workLoad;
				}
			}
			
			if (bestIp == null) {
				/* all the optimal task trackers are full, pick a nearest one */
				long baseIp = ipToLong(entries.get(0).dataNodeRegistryIP);
				long minDist = Long.MAX_VALUE;
				Set<String> allNodes = taskTrackerToTasksTbl.keySet();
				for (String ip : allNodes) {
					int workLoad = taskTrackerToTasksTbl.get(ip).size();
					if (workLoad < minLoad && workLoad + 1 <= MapReduce.TaskTracker.MAX_NUM_MAP_TASK 
							&& taskTrackerTbl.get(ip).getStatus() == TaskTrackerInfo.Status.RUNNING) {
						long thisIp = ipToLong(ip);
						long dist = Math.abs(thisIp - baseIp);
						if (dist < minDist) {
							bestIp = ip;
							minDist = dist;
						}
					}
				}
			}
			
			//TODO:still cannot find a slot-> all task trackers are full
			taskTrackerToTasksTbl.get(bestIp).add(task);
		}
		
		public synchronized TaskTrackerInfo pickTaskTrackerForMap(MapperTask task) {
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
			long baseIp = ipToLong(nodes.get(0).dataNodeRegistryIP);
			long minDist = Long.MAX_VALUE;
			String nearestIp = null;
			Set<String> allNodes = taskTrackerTbl.keySet();
			for (String ip : allNodes) {
				TaskTrackerInfo taskTrackerInfo = taskTrackerTbl.get(ip);
				if (taskTrackerInfo.getStatus() == TaskTrackerInfo.Status.RUNNING && taskTrackerInfo.getMapSlot() > 0) {
					long thisIp = ipToLong(ip);
					long dist = Math.abs(thisIp - baseIp);
					if (dist < minDist) {
						nearestIp = ip;
						minDist = dist;
					}
				}
			}
			if (nearestIp != null) {
				return taskTrackerTbl.get(nearestIp);
			} else {
				/* all tasktrackers are full */
				return null;
			}
		}
		
		private long ipToLong(String ip) {
			String newIp = ip.replace(".", "");
			long ipInt = Long.parseLong(newIp);
			return ipInt;
		}
	}
	
}
