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
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import mapreduce.Job;
import mapreduce.io.Split;
import mapreduce.task.MapperTask;
import mapreduce.task.PartitionEntry;
import mapreduce.task.ReducerTask;
import mapreduce.task.Task;
import mapreduce.task.TaskTrackerRemoteInterface;

public class JobTracker implements JobTrackerRemoteInterface {
	
	private static int MAX_NUM_MAP_TASK = 999;
	
	private long taskNaming = 1;
	
	private JobScheduler jobScheduler;
	
	/* host IP, task tracker port */
	private ConcurrentHashMap<String, TaskTrackerInfo> taskTrackerTbl = new ConcurrentHashMap<String, TaskTrackerInfo>();
	
	/* keep jobs in this tbl after submission from jobclient */
	private ConcurrentHashMap<String, Job> jobTbl = new ConcurrentHashMap<String, Job>();
	
	private ConcurrentHashMap<String, Task> taskTbl = new ConcurrentHashMap<String, Task>();
	
	private ConcurrentHashMap<String, JobStatus> jobStatusTbl = new ConcurrentHashMap<String, JobStatus>();
	
	
	public void init() {
		jobScheduler = new JobScheduler();
		//TimerTask taskTrackerCheck = new TaskTrackerCheck();
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
			TaskTrackerInfo stat = new TaskTrackerInfo(ip, port/*, mapSlots, reduceSlots*/);
			taskTrackerTbl.put(ip, stat);
			//this.jobScheduler.taskScheduleTbl.put(ip, new PriorityQueue<Task>(MAX_NUM_MAP_TASK, new SchedulorComparator()));
			this.jobScheduler.taskScheduleTbl.put(ip, new PriorityQueue<Task>(MAX_NUM_MAP_TASK, new Comparator<Task>() {
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
						long taskId1 = Long.parseLong(t1.getTaskId());
						long taskId2 = Long.parseLong(t2.getTaskId());
						if (taskId1 > taskId2) {
							return 1;
						} else {
							return -1;
						}
					}
				}
			}));
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
	
	private MapperTask createMapTask(String jobId, int level, Split split, Class<?> theClass, int partitionNum) {
		MapperTask task = new MapperTask(nameTask(), jobId, level, split, theClass, partitionNum);
		return task;
	}
	
	private ReducerTask createReduceTask(String jobId, int level, int reducerSEQ, Class<?> theClass, PartitionEntry[] partitionEntry, String path) {
		ReducerTask task = new ReducerTask(nameTask(), jobId, level, reducerSEQ, theClass, partitionEntry, path);
		return task;
	}
	
	private synchronized String nameTask() {
		long taskName = taskNaming++;
		return Long.toString(taskName);
	}
	
	/**
	 * JobClient calls this method to submit a job to schedule
	 */
	@Override
	public synchronized String submitJob(Job job) {
		String jobId = String.format("%d", new Date().getTime());
		job.setJobId(jobId);
		jobTbl.put(jobId, job);
		
		/* initialize job status record */
		JobStatus jobStatus = new JobStatus(job.getSplit().size(), job.getJobConf().getNumReduceTasks());
		if (Hdfs.DEBUG) {
			System.out.println("DEBUG JobTracker.submitJob() numReduceTasks = " + job.getJobConf().getNumReduceTasks());
		}
		this.jobStatusTbl.put(jobId, jobStatus);
		System.out.println("DEBUG JobTracker.submitJob() jobId: " + jobId);
		initMapTasks(job);
		
		return jobId;
	}
	
	/**
	 * Initialize the job by splitting it into multiple map tasks, pushing
	 * initialized tasks to job scheduler for scheduling
	 * @param job The job to initialize
	 */
	private synchronized void initMapTasks(Job job) {
		for (Split split : job.getSplit()) {
			MapperTask task = 
					createMapTask(job.getJobId(), job.getJobConf().getPriority(), split, job.getJobConf().getMapper(), job.getJobConf().getNumReduceTasks());
			
			if (Hdfs.DEBUG) {
				System.out.println("DEBUG JobTracker.addMapTasks(): now adding task " + task.getTaskId() + " to Task Queue");
			}
			
			this.taskTbl.put(task.getTaskId(), task);
			/* TODO: initiate another thread to add all tasks */
			this.jobScheduler.addMapTask(task);
			/* initialize task status record */
			TaskStatus stat = new TaskStatus(job.getJobId(), task.getTaskId(), WorkStatus.RUNNING, null, -1);
			this.jobStatusTbl.get(job.getJobId()).mapperStatusTbl.put(task.getTaskId(), stat);	
		}
		if (Hdfs.DEBUG) {
			System.out.println("DEBUG JobTrakcer.initMapTask(): map tasks initialization finished, current job scheduling queue: ");
			this.jobScheduler.printScheduleTbl();
		}
	}
	
	private void initReduceTasks(String jobId) {
		Job job = this.jobTbl.get(jobId);
		initReduceTasks(job);
	}
	
	private void initReduceTasks(Job job) {
		int numOfReducer = job.getJobConf().getNumReduceTasks();
		if (Hdfs.DEBUG) {
			System.out.println("DEBUG JobTracker.initReduceTasks() numReduceTasks = " + numOfReducer);
		}
		ConcurrentHashMap<String, TaskStatus> tbl = this.jobStatusTbl.get(job.getJobId()).mapperStatusTbl;
		Set<String> hosts = new HashSet<String>();
		Set<String> set = tbl.keySet();	
		/* create partition entry array */
		PartitionEntry[] entries = new PartitionEntry[set.size()];
		int i = 0;
		for (String taskId : set) {
			entries[i++] = new PartitionEntry(taskId, tbl.get(taskId).taskTrackerIp, MapReduce.TaskTracker.taskTrackerServerPort);
		}
		
		/* create reducer tasks */
		for (int j = 0; j < numOfReducer; j++) {
			ReducerTask task = createReduceTask(job.getJobId(), job.getJobConf().getPriority(), j, job.getJobConf().getReducerClass(), entries, job.getJobConf().getOutputPath());
			this.taskTbl.put(task.getTaskId(), task);
			this.jobScheduler.addReduceTask(task);
			TaskStatus stat = new TaskStatus(job.getJobId(), task.getTaskId(), WorkStatus.RUNNING, null, -1);
			this.jobStatusTbl.get(job.getJobId()).reducerStatusTbl.put(task.getTaskId(), stat);
		}
	}
	
	
	@Override
	public List<Task> heartBeat(TaskTrackerReport report) {
		if (Hdfs.DEBUG) {
			System.out.println("DEBUG JobTracker.heartBeat(): Receive TaskTrackerReport from " + report.taskTrackerIp);
		}
		
		//TODO: leave for parallel
		
		this.taskTrackerTbl.get(report.taskTrackerIp).updateTimeStamp();
		List<TaskStatus> allStatus = report.taskStatus;
		if (allStatus != null) {
			for (TaskStatus taskStatus : allStatus) {
				/* update taskStatus */
				//TODO: SERVERAL CASES EXIST!
				updateTaskStatus(taskStatus);
			}
		}
		
		/* assign a number of tasks back to task tracker */
		Queue<Task> allTasks = this.jobScheduler.taskScheduleTbl.get(report.taskTrackerIp);
		List<Task> assignment = new ArrayList<Task>();
		if (report.emptySlot > 0 && allTasks.size() != 0) {
			int queueSize = allTasks.size();
			for (int i = 0; i < report.emptySlot && i < queueSize; i++) {
				assignment.add(allTasks.poll());
			}
		}
		
		/* update tasks status */
		TaskTrackerInfo taskTracker = this.taskTrackerTbl.get(report.taskTrackerIp);
		taskTracker.addTask(assignment);
		
		return assignment;
	}
	
	/**
	 * Given a task status of a mapper task, update the status in corresponding
	 * entry. Upon task failed, push to job scheduler to schedule again
	 * @param taskStatus
	 */
	public void updateTaskStatus(TaskStatus taskStatus) {
		boolean isMapper = this.taskTbl.get(taskStatus.taskId) instanceof MapperTask;
		JobStatus jobStatus = this.jobStatusTbl.get(taskStatus.jobId);
		synchronized(jobStatus) {
			TaskStatus preStatus = isMapper ? jobStatus.mapperStatusTbl.get(taskStatus.taskId) : jobStatus.reducerStatusTbl.get(taskStatus.taskId);
			
			if (preStatus.status == WorkStatus.SUCCESS) {
				/* task already success, do nothing */
				return;
			}
			/* previous Status: RUNNING or FAILED */
			if (isMapper) {
				jobStatus.mapperStatusTbl.put(taskStatus.taskId, taskStatus);
			} else {
				jobStatus.reducerStatusTbl.put(taskStatus.taskId, taskStatus);
			}
			
			
			if (taskStatus.status == WorkStatus.SUCCESS) {
				if (Hdfs.DEBUG) {
					System.out.print("DEBUG JobTracker.updateTaskStatus(): Task " + taskStatus.taskId + " in job " + taskStatus.jobId + " SUCCESS, ");
				}
				if (isMapper) {
					System.out.println("map task");
					jobStatus.mapTaskLeft--;
					if (jobStatus.mapTaskLeft == 0) {
						System.out.println("DEBUG schedule reducer task");
						/* schedule corresponding reducer task */
						initReduceTasks(taskStatus.jobId);
					}
				} else {
					System.out.println("reduce task");
					jobStatus.reduceTaskLeft--;
				}			
			} else if (taskStatus.status == WorkStatus.FAILED) {
				/* The task will be scheduled again so the intermediate result may not be on this TaskTracker */
				TaskTrackerInfo taskTracker = this.taskTrackerTbl.get(taskStatus.taskTrackerIp);
				taskTracker.removeTask(taskStatus.taskId);
				
				if (Hdfs.DEBUG) {
					System.out.println("DEBUG JobTracker.updateTaskStatus(): Task " + taskStatus.taskId + " in job " + taskStatus.jobId + " FAILED ");
				}
				/* try to re-schedule this task */
				Task task = this.taskTbl.get(taskStatus.taskId);
				if (task.getRescheduleNum() < MapReduce.JobTracker.MAX_RESCHEDULE_ATTEMPTS) {
					/* increase schedule priority of this task */
					task.increasePriority();
					task.increaseRescheuleNum();
					if (isMapper) {
						this.jobScheduler.addMapTask((MapperTask) task);
					} else {
						this.jobScheduler.addReduceTask((ReducerTask) task);
					}
				} else {
					/* reach max task reschedule limit, task failed / job failed */
					System.out.println("                                     Task " + taskStatus.taskId + " cannot be rescheduled anymore");
				}

			}
		}
	}
	
	@Override
	public int checkMapProgress(String jobId) throws RemoteException {
		return this.jobStatusTbl.get(jobId).mapTaskLeft;
	}

	@Override
	public int checkReduceProgress(String jobId) throws RemoteException {
		return this.jobStatusTbl.get(jobId).reduceTaskLeft;
	}
	
	/* JobScheduler */
	private class JobScheduler {
		
		public ConcurrentHashMap<String, Queue<Task>> taskScheduleTbl = new ConcurrentHashMap<String, Queue<Task>>();
		
		public class SchedulorComparator implements Comparator<Task> {

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
					long taskId1 = Long.parseLong(t1.getTaskId());
					long taskId2 = Long.parseLong(t2.getTaskId());
					if (taskId1 > taskId2) {
						return 1;
					} else {
						return -1;
					}
				}
			}

			
		}
		
		/**
		 * Schedule a map task with data locality first
		 * @param task The task to schedule
		 */
		public synchronized void addMapTask(MapperTask task) {
			int chunkIdx = task.getSplit().getChunkIdx();
			List<DataNodeEntry> entries = task.getSplit().getFile().getChunkList().get(chunkIdx).getAllLocations();
			/* pick the one among all entries with lightest work-load */
			String bestIp = null;
			int minLoad = Integer.MAX_VALUE;
			for (DataNodeEntry entry : entries) {
				if (taskScheduleTbl.containsKey(entry.dataNodeRegistryIP)) {
					int workLoad = taskScheduleTbl.get(entry.dataNodeRegistryIP).size();
					if (workLoad < minLoad && workLoad < MapReduce.TaskTracker.MAX_NUM_MAP_TASK 
							&& taskTrackerTbl.get(entry.dataNodeRegistryIP).getStatus() == TaskTrackerInfo.Status.RUNNING) {
						bestIp = entry.dataNodeRegistryIP;
						minLoad = workLoad;
					}
				}
			}
			
//			if (bestIp == null) {
//				/* all the optimal task trackers are full, pick a nearest one */
//				long baseIp = ipToLong(entries.get(0).dataNodeRegistryIP);
//				long minDist = Long.MAX_VALUE;
//				Set<String> allNodes = taskScheduleTbl.keySet();
//				for (String ip : allNodes) {
//					int workLoad = taskScheduleTbl.get(ip).size();
//					if (workLoad < minLoad && workLoad < MapReduce.TaskTracker.MAX_NUM_MAP_TASK 
//							&& taskTrackerTbl.get(ip).getStatus() == TaskTrackerInfo.Status.RUNNING) {
//						long thisIp = ipToLong(ip);
//						long dist = Math.abs(thisIp - baseIp);
//						if (dist < minDist) {
//							bestIp = ip;
//							minDist = dist;
//						}
//					}
//				}
//			}
			if (bestIp == null) {
				/* all the optimal task trackers are full, pick a task tracker with lightest workload among all */
				minLoad = Integer.MAX_VALUE;
				Set<String> allNodes = taskScheduleTbl.keySet();
				for (String ip : allNodes) {
					int workLoad = taskScheduleTbl.get(ip).size();
					if (taskTrackerTbl.get(ip).getStatus() == TaskTrackerInfo.Status.RUNNING && workLoad < minLoad) {
						minLoad = workLoad;
						bestIp = ip;
						/* push idle task tracker to work! */
						if (workLoad == 0) {
							break;
						}
					}
				}
			}
			
			taskScheduleTbl.get(bestIp).add(task);
		}

		
		/**
		 * Schedule a reduce task, idle task tracker first
		 * @param task The task to schedule
		 */
		public synchronized void addReduceTask(ReducerTask task) {
			if (Hdfs.DEBUG) {
				System.out.println("DEBUG JobTracker.Scheduler.addReduceTask(): add reduce task, id = " + task.getTaskId());
			}
			/* pick the one with lightest workload */
			String bestIp = null;
			int minLoad = Integer.MAX_VALUE;
			Set<String> allNodes = taskScheduleTbl.keySet();
			for (String ip : allNodes) {
				int workLoad = taskScheduleTbl.get(ip).size();
				if (workLoad == 0) {
					bestIp = ip;
					break;
				} else {
					if (workLoad < minLoad) {
						bestIp = ip;
						minLoad = workLoad;
					}
				}
			}
			taskScheduleTbl.get(bestIp).add(task); 
		}
		
//		public synchronized TaskTrackerInfo pickTaskTrackerForMap(MapperTask task) {
//			int chunkIdx = task.getSplit().getChunkIdx();
//			List<DataNodeEntry> nodes = task.getSplit().getFile().getChunkList().get(chunkIdx).getAllLocations();
//			for (DataNodeEntry node : nodes) {
//				String ip = node.dataNodeRegistryIP;
//				if (taskTrackerTbl.containsKey(ip)) {
//					TaskTrackerInfo taskTrackerInfo = taskTrackerTbl.get(ip);
//					if (taskTrackerInfo.getStatus() == TaskTrackerInfo.Status.RUNNING && 
//							taskTrackerInfo.getMapSlot() > 0) {
//						return taskTrackerInfo;
//					}
//				}
//			}
//			
//			/* all data nodes with that split locally is busy now, pick a nearest one */
//			long baseIp = ipToLong(nodes.get(0).dataNodeRegistryIP);
//			long minDist = Long.MAX_VALUE;
//			String nearestIp = null;
//			Set<String> allNodes = taskTrackerTbl.keySet();
//			for (String ip : allNodes) {
//				TaskTrackerInfo taskTrackerInfo = taskTrackerTbl.get(ip);
//				if (taskTrackerInfo.getStatus() == TaskTrackerInfo.Status.RUNNING && taskTrackerInfo.getMapSlot() > 0) {
//					long thisIp = ipToLong(ip);
//					long dist = Math.abs(thisIp - baseIp);
//					if (dist < minDist) {
//						nearestIp = ip;
//						minDist = dist;
//					}
//				}
//			}
//			if (nearestIp != null) {
//				return taskTrackerTbl.get(nearestIp);
//			} else {
//				/* all tasktrackers are full */
//				return null;
//			}
//		}
		
		private long ipToLong(String ip) {
			String newIp = ip.replace(".", "");
			long ipInt = Long.parseLong(newIp);
			return ipInt;
		}
		
		private void printScheduleTbl() {
			Set<String> taskTrackers = this.taskScheduleTbl.keySet();
			for (String each : taskTrackers) {
				System.out.println(">>>>>>>>>>>>>>>>>>>>>");
				System.out.println("TaskTracker: " + each + " assigned tasks: ");
				Queue<Task> tasks = this.taskScheduleTbl.get(each);
				for (Task task : tasks) {
					System.out.println("TaskId: " + task.getTaskId() + " jobId: " + task.getJobId() + " class name: " + task.getClass().getName());
				}
				System.out.println("<<<<<<<<<<<<<<<<<<<<<");
			}
		}	
	}
	
	private class TaskTrackerCheck extends TimerTask {
		
		@Override
		public void run() {
			Set<String> taskTrackers = JobTracker.this.taskTrackerTbl.keySet();
			for (String taskTrackerIp : taskTrackers) {
				long lastHeartBeat = JobTracker.this.taskTrackerTbl.get(taskTrackerIp).getTimeStamp();
				if (System.currentTimeMillis() - lastHeartBeat >= MapReduce.JobTracker.TASK_TRACKER_EXPIRATION) {
					JobTracker.this.taskTrackerTbl.get(taskTrackerIp).disable();
					/* re-schedule all tasks previously complete and currently running on this TaskTracker */
					/* A. re-schedule tasks in associative scheduling queue */
					//synchronized(JobTracker.this.jobScheduler.taskScheduleTbl) {
					Queue<Task> tasks = JobTracker.this.jobScheduler.taskScheduleTbl.get(taskTrackerIp);
					for (Task task : tasks) {
						if (task instanceof MapperTask) {
							JobTracker.this.jobScheduler.addMapTask((MapperTask) task);
						} else {
							JobTracker.this.jobScheduler.addReduceTask((ReducerTask) task);
						}
					}
					
					//}
					/* B. re-schedule related tasks on this TaskTracker */
				}
			}
			
		}
		
	}
}
