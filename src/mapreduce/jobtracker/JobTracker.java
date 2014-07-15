package mapreduce.jobtracker;

import hdfs.DataStructure.DataNodeEntry;
import global.Hdfs;
import global.MapReduce;

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
import mapreduce.io.Split;
import mapreduce.task.MapperTask;
import mapreduce.task.ReducerTask;
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
		Thread t = new Thread(new JobScheduler());
		t.start();
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
			boolean ret = taskTrackerStub.runTask(task);
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (NotBoundException e) {
			e.printStackTrace();
		}
	}
	
	private Task createMapTask(String jobId, Split split, Class<?> theClass, int partitionNum) {
		Task task = new MapperTask(nameTask(), jobId, split, theClass, partitionNum);
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
			if (Hdfs.DEBUG) {
				System.out.println("DEBUG JobTracker.addMapTasks(): now adding task " + task.getTaskId() + " to Task Queue");
			}
			this.taskQueue.add(task);
		}
	}
	
	
	/* a FIFO JobScheduler */
	private class JobScheduler implements Runnable{

		@Override
		public void run() {
			if (Hdfs.DEBUG) {
				System.out.println("DEBUG JobTracker.JobScheduler.run()");
			}
			// TODO Auto-generated method stub
			while (true) {
				Task nextTask = taskQueue.peek();
				
				if (nextTask instanceof MapperTask) {
					if (Hdfs.DEBUG) {
						System.out.println("DEBUG JobTracker.JobScheduler.run() now scheduling task " + nextTask.getTaskId());
					}
					/* schedule a map task */
					TaskTrackerInfo taskTracker = pickTaskTrackerForMap((MapperTask)nextTask);
					if (taskTracker != null) {
						submitTask(taskQueue.poll(), taskTracker.getIp(), taskTracker.getPort());
					}
				} else if (nextTask instanceof ReducerTask){
					/* schedule a reduce task */
				}
			}
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
