package mapreduce.jobtracker;

import global.Hdfs;
import global.MapReduce;
import hdfs.DataStructure.DataNodeEntry;
import hdfs.NameNode.NameNodeRemoteInterface;

import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;

import mapreduce.Job;
import mapreduce.io.Split;
import mapreduce.task.CleanerTask;
import mapreduce.task.MapperTask;
import mapreduce.task.PartitionEntry;
import mapreduce.task.ReducerTask;
import mapreduce.task.Task;

public class JobTracker implements JobTrackerRemoteInterface {
	
	private static int MAX_NUM_MAP_TASK = 999;
	
	private long taskNaming = 1;
	
	private JobScheduler jobScheduler;
	
	/* host IP, task tracker port */
	private AbstractMap<String, TaskTrackerInfo> taskTrackerTbl = new ConcurrentHashMap<String, TaskTrackerInfo>();
	
	/* keep jobs in this tbl after submission from jobclient */
	private AbstractMap<String, Job> jobTbl = new ConcurrentHashMap<String, Job>();
	
	private AbstractMap<String, Task> taskTbl = new ConcurrentHashMap<String, Task>();
	
	private AbstractMap<String, JobStatus> jobStatusTbl = new ConcurrentHashMap<String, JobStatus>();
	
//	public static void main(String[] args) {
//		JobTracker jt = new JobTracker();
//		jt.init();
//		if (Hdfs.Core.DEBUG) {
//			System.out.println("DEBUG runJobTracker.main(): jobTracker now running");
//		}
//	}
	
	public void init() {
		jobScheduler = new JobScheduler();
		Thread t = new Thread(new TaskTrackerCheck());
		t.start();
		//TimerTask taskTrackerCheck = new TaskTrackerCheck();
		
		try {
			Registry jtRegistry = LocateRegistry.createRegistry(MapReduce.Core.JOB_TRACKER_REGISTRY_PORT);
			JobTrackerRemoteInterface jtStub = (JobTrackerRemoteInterface) UnicastRemoteObject.exportObject(this, 0);
			jtRegistry.rebind(MapReduce.Core.JOB_TRACKER_SERVICE_NAME, jtStub);
		} catch (RemoteException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public String join(String ip, int port, int serverPort, int numSlots) {
		String taskTrackerName = ip + ":" + port;
		
		if (!taskTrackerTbl.containsKey(ip)) {
			TaskTrackerInfo stat = new TaskTrackerInfo(ip, port, serverPort, numSlots);
			taskTrackerTbl.put(ip, stat);
			this.jobScheduler.taskScheduleTbl.put(ip, new PriorityBlockingQueue<Task>(MAX_NUM_MAP_TASK, new SchedulerComparator()));
		}

		if (Hdfs.Core.DEBUG) {
			System.out.println("DEBUG JobTracker.join(): TaskTracker " + taskTrackerName + " join cluster");
		}
		//TODO: upon a tasktracker recover from failure, what about those tasks assigned on it?
		return taskTrackerName;
	}
	
	private MapperTask createMapTask(String jobId, int level, Split split, Class<?> theClass, int partitionNum) {
		MapperTask task = new MapperTask(jobId, nameTask(), level, split, theClass, partitionNum);
		//TODO: by default the status is READY?
		return task;
	}
	
	private ReducerTask createReduceTask(String jobId, int level, int reducerSEQ, Class<?> theClass, PartitionEntry[] partitionEntry, String path) {
		ReducerTask task = new ReducerTask(jobId, nameTask(), level, reducerSEQ, theClass, partitionEntry, path + "-" + "part" + "-" + reducerSEQ);
		return task;
	}
	
	private synchronized String nameTask() {
		long taskName = taskNaming++;
		String name = String.format("task%05d", taskName);
		return name;
	}
	
	private synchronized String nameJob() {
		return String.format("%d", new Date().getTime());
	}
	
	/**
	 * JobClient calls this method to submit a job to schedule
	 */
	@Override
	public String submitJob(Job job) {
		String jobId = nameJob();
		job.setJobId(jobId);
		jobTbl.put(jobId, job);
		
		/* initialize job status record */
		initJob(job);
		System.out.println("DEBUG JobTracker.submitJob() jobId: " + jobId);
		initMapTasks(job);
		
		return jobId;
	}
	
	
	private void initJob(Job job) {
		JobStatus jobStatus = new JobStatus(job.getJobId(), job.getJobConf().getJobName(), job.getSplit().size(), job.getJobConf().getNumReduceTasks());
		if (Hdfs.Core.DEBUG) {
			System.out.println("DEBUG JobTracker.submitJob() numReduceTasks = " + job.getJobConf().getNumReduceTasks());
		}
		this.jobStatusTbl.put(job.getJobId(), jobStatus);
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
			
			if (Hdfs.Core.DEBUG) {
				System.out.println("DEBUG JobTracker.addMapTasks(): now adding task " + task.getTaskId() + " to Task Queue");
			}
			
			this.taskTbl.put(task.getTaskId(), task);
			/* TODO: initiate another thread to add all tasks */
			this.jobScheduler.addMapTask(task);
			/* initialize task status record */
			TaskStatus stat = new TaskStatus(job.getJobId(), task.getTaskId(), WorkStatus.RUNNING, null, -1);
			this.jobStatusTbl.get(job.getJobId()).mapperStatusTbl.put(task.getTaskId(), stat);	
		}
		if (Hdfs.Core.DEBUG) {
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
		if (Hdfs.Core.DEBUG) {
			System.out.println("DEBUG JobTracker.initReduceTasks() numReduceTasks = " + numOfReducer);
		}
		AbstractMap<String, TaskStatus> mapperStatusTbl = this.jobStatusTbl.get(job.getJobId()).mapperStatusTbl;
		Set<String> mapIdSet = mapperStatusTbl.keySet();	
		
		/* create partition entry array */
		PartitionEntry[] entries = new PartitionEntry[mapIdSet.size()];
		int i = 0;

		
		for (String mapTaskId : mapIdSet) {
			String taskTrackerIp = mapperStatusTbl.get(mapTaskId).taskTrackerIp;
			entries[i++] = new PartitionEntry(mapTaskId, taskTrackerIp, this.taskTrackerTbl.get(taskTrackerIp).getServerPort());
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
	public JobTrackerACK heartBeat(TaskTrackerReport report) {
//		if (Hdfs.DEBUG) {
//			System.out.println("DEBUG JobTracker.heartBeat(): Receive TaskTrackerReport from " + report.taskTrackerIp);
//		}
		
		//TODO: leave for parallel
		
		this.taskTrackerTbl.get(report.taskTrackerIp).updateTimeStamp();
		this.taskTrackerTbl.get(report.taskTrackerIp).enable();
		List<TaskStatus> allStatus = report.taskStatus;
		
		/* acknowledge those FAILED and SUCCESS tasks */
		List<TaskStatus> ackTasks = new ArrayList<TaskStatus>();
		if (allStatus != null) {
			for (TaskStatus taskStatus : allStatus) {
				if (this.jobStatusTbl.get(taskStatus.jobId).status
						!= WorkStatus.RUNNING) {
					/* if the job is not RUNNING, discard further task update (from network partition etc.)*/
					continue;
				}
				
				/* update taskStatus */
				//TODO: SERVERAL CASES EXIST!
				updateTaskStatus(taskStatus);
				if (taskStatus.status == WorkStatus.FAILED || taskStatus.status == WorkStatus.SUCCESS) {
					ackTasks.add(taskStatus);
				}
			}
		}
		
		/* assign a number of tasks back to task tracker */
		Queue<Task> allTasks = this.jobScheduler.taskScheduleTbl.get(report.taskTrackerIp);
		List<Task> assignment = new ArrayList<Task>();
		if (report.emptySlot > 0 && allTasks.size() != 0) {
			int queueSize = allTasks.size();
			for (int i = 0; i < report.emptySlot && i < queueSize; i++) {
				Task task = allTasks.poll();
				assignment.add(task);
				/* keep taskTracker ip into this task's status entry */
				if (task instanceof MapperTask) {
					this.jobStatusTbl.get(task.getJobId()).mapperStatusTbl.get(task.getTaskId()).taskTrackerIp =
							report.taskTrackerIp;
				} else if (task instanceof ReducerTask) {
					this.jobStatusTbl.get(task.getJobId()).reducerStatusTbl.get(task.getTaskId()).taskTrackerIp =
							report.taskTrackerIp;
				}
			}
		}
		
		/* update tasks status */
		TaskTrackerInfo taskTracker = this.taskTrackerTbl.get(report.taskTrackerIp);
		
		taskTracker.addTask(assignment);
		
		JobTrackerACK ack = new JobTrackerACK(assignment, ackTasks);
		
		return ack;
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
			
			if ( preStatus == null || preStatus.status == WorkStatus.SUCCESS) {
				/* task already discard or success, do nothing */
				return;
			}
			/* previous Status: RUNNING or FAILED */
			if (isMapper) {
				jobStatus.mapperStatusTbl.put(taskStatus.taskId, taskStatus);
			} else {
				jobStatus.reducerStatusTbl.put(taskStatus.taskId, taskStatus);
			}
			
			
			if (taskStatus.status == WorkStatus.SUCCESS) {
				if (Hdfs.Core.DEBUG) {
					System.out.print("DEBUG JobTracker.updateTaskStatus(): Task " + taskStatus.taskId + " in job " + taskStatus.jobId + " SUCCESS, on TaskTracker " + taskStatus.taskTrackerIp);
				}
				if (isMapper) {
					System.out.println(" map task");
					jobStatus.mapTaskLeft--;
					if (jobStatus.mapTaskLeft == 0) {
						System.out.println("DEBUG schedule reducer task");
						/* schedule corresponding reducer task */
						initReduceTasks(taskStatus.jobId);
					}
				} else {
					System.out.println(" reduce task");
					jobStatus.reduceTaskLeft--;
					/* if job finished, check if all reducers are SUCCESS,
					 * otherwise restart the whole job */
					if (jobStatus.reduceTaskLeft == 0 ) {
						if (!reducerAllSuccess(jobStatus.reducerStatusTbl)) {
							resetJob(jobStatus);
						} else {
							/* job success */
							jobStatus.status = WorkStatus.SUCCESS;
							//TODO: CLEAN UP ALL THE INTERMEDIATE FILES
							//cleanUp(jobStatus.jobId);
						}
						
					}
					
				}			
			} else if (taskStatus.status == WorkStatus.FAILED) {
				/* The task will be scheduled again so the intermediate result may not be on this TaskTracker */
				TaskTrackerInfo taskTracker = this.taskTrackerTbl.get(taskStatus.taskTrackerIp);
				taskTracker.removeTask(taskStatus.taskId);
				
				if (Hdfs.Core.DEBUG) {
					System.out.println("DEBUG JobTracker.updateTaskStatus(): Task " + taskStatus.taskId + " in job " + taskStatus.jobId + " FAILED, on TaskTracker " + taskStatus.taskTrackerIp);
				}
				/* try to re-schedule this task */
				Task task = this.taskTbl.get(taskStatus.taskId);
				if (task.getRescheduleNum() < MapReduce.JobTracker.MAX_RESCHEDULE_ATTEMPTS) {				
					task.increasePriority();
					task.increaseRescheuleNum();
					if (isMapper) {
						System.out.println(" map task");
						/* increase schedule priority of this task */
						/* disable the bad task tracker so that the failed task
						 * will not be scheduled to this TaskTracker */
						TaskTrackerInfo badTaskTracker = taskTrackerTbl.get(taskStatus.taskTrackerIp);
						badTaskTracker.disable();
						this.jobScheduler.addMapTask((MapperTask) task);
						badTaskTracker.enable();
					} else {
						//TODO:re-schedule the whole job
						System.out.println(" reduce task");
						jobStatus.reduceTaskLeft--;
						/* if job finished, check if all reducers are SUCCESS,
						 * otherwise restart the whole job */
						if (jobStatus.reduceTaskLeft == 0 /*&& !reducerAllSuccess(jobStatus.reducerStatusTbl)*/) {
							resetJob(jobStatus);
						}
					}					
					
				} else {
					/* reach max task reschedule limit, task failed / job failed */
					System.out.println("DEBUG JobTracker.updateTaskStatus(): Task " + taskStatus.taskId + " in Job " + taskStatus.jobId + " cannot be rescheduled anymore");
					/* mark this job as failed */
					jobStatus.status = WorkStatus.FAILED;
				}

			}
		}
	}
	
	private boolean reducerAllSuccess(AbstractMap<String, TaskStatus> reducerStatusTbl) {
		Set<String> taskId = reducerStatusTbl.keySet();
		for (String id : taskId) {
			if (reducerStatusTbl.get(id).status == WorkStatus.FAILED) {
				if (Hdfs.Core.DEBUG) {
					System.out.println("DEBUG JobTracker.reducerAllSuccess(): FAILED reducer task found, taskId: " + id);
				}
				return false;
			}
		}
		return true;
	}
	
	private void resetJob(JobStatus jobStatus) {
		if (jobStatus.rescheduleNum >= MapReduce.JobTracker.MAX_RESCHEDULE_ATTEMPTS) {
			jobFail(jobStatus.jobId);
			return;
		}
		jobStatus.rescheduleNum++;
		jobStatus.mapTaskLeft = jobStatus.mapTaskTotal;
		jobStatus.reduceTaskLeft = jobStatus.reduceTaskTotal;
		/* delete result on HDFS from reducer */
		try {
			Registry nameNodeRegistry = LocateRegistry.getRegistry(Hdfs.Core.NAME_NODE_IP, Hdfs.Core.NAME_NODE_REGISTRY_PORT);
			NameNodeRemoteInterface nameNodeStub = (NameNodeRemoteInterface) nameNodeRegistry.lookup(Hdfs.Core.NAME_NODE_SERVICE_NAME);
			String outputPath = this.jobTbl.get(jobStatus.jobId).getJobConf().getOutputPath();
			for (int i = 0; i < jobStatus.reduceTaskTotal; i++) {
				try {
					nameNodeStub.delete(String.format("%s-part-%d", outputPath, i));
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NotBoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		jobStatus.reducerStatusTbl = new ConcurrentHashMap<String, TaskStatus>();
		Set<String> mapTaskIds = jobStatus.mapperStatusTbl.keySet();
		for (String id : mapTaskIds) {
			jobStatus.mapperStatusTbl.get(id).status = WorkStatus.RUNNING;
			this.jobScheduler.addMapTask((MapperTask) this.taskTbl.get(id));
		}
	}
	
	/**
	 * Upon job failure, set the status of the job as FAILED so that the client
	 * will get notified eventually
	 * @param jobId
	 */
	private void jobFail(String jobId) {
		this.jobStatusTbl.get(jobId).status = WorkStatus.FAILED;
	}
	
	public void terminateJob(String jobId) {
		this.jobStatusTbl.get(jobId).status = WorkStatus.TERMINATED;
	}
	
	private void cleanUp(String jobId) {
		
		/* init mappers' inermediate files' prefix */
		HashMap<String, List<String>> mapClean = 
				new HashMap<String, List<String>>();
		
		AbstractMap<String, TaskStatus> mapperStatusTbl = 
				this.jobStatusTbl.get(jobId).mapperStatusTbl;
		
		Set<String> mapTaskIds = mapperStatusTbl.keySet();
		for (String mapTaskId : mapTaskIds) {
			
			String taskTrackerIp = mapperStatusTbl.get(mapTaskId).taskTrackerIp;
			
			List<String> ids = mapClean.get(taskTrackerIp);
			
			String mapFilePrefix = jobId + "-" + mapTaskId;
			
			if (ids == null) {
				List<String> prefixList = new LinkedList<String>();
				prefixList.add(mapFilePrefix);
				mapClean.put(taskTrackerIp, prefixList);
				
			} else {
				ids.add(mapFilePrefix);
			}
		}
		
		/* init reducers' intermediate files' prefix */
		HashMap<String, List<String>> reduceClean = 
				new HashMap<String, List<String>>();
		
		AbstractMap<String, TaskStatus> reducerStatusTbl =
				this.jobStatusTbl.get(jobId).reducerStatusTbl;
		
		Set<String> reduceTaskIds = reducerStatusTbl.keySet();
		for (String reduceTaskId : reduceTaskIds) {
			ReducerTask reducerTask = (ReducerTask) this.taskTbl.get(reduceTaskId);
			PartitionEntry[] entries = reducerTask.getEntries();
			
			for (PartitionEntry entry : entries) {
				/* reducer's intermediate file: jid-tid-mapTaskId */
				String reduceFileName = jobId + "-" + reduceTaskId + "-" + entry.getTID();
				
				if (!reduceClean.containsKey(entry.getHost())) {
					List<String> fileNames = new LinkedList<String>();
					fileNames.add(reduceFileName);
					reduceClean.put(entry.getHost(), fileNames);
				} else {
					reduceClean.get(entry.getHost()).add(reduceFileName);
				}
			}
			
		}
		
		/* init all cleaner tasks */
		HashMap<String, CleanerTask> cleanTaskTbl = new HashMap<String, CleanerTask>();
		
		Set<String> hostIps = mapClean.keySet();
		for (String hostIp : hostIps) {
			CleanerTask cleanerTask = new CleanerTask(hostIp, jobId, reduceTaskIds.size());
			cleanerTask.addMapperFile(mapClean.get(hostIp));
			cleanTaskTbl.put(hostIp, cleanerTask);
		}
		
		hostIps = reduceClean.keySet();
		for (String hostIp : hostIps) {
			if (!cleanTaskTbl.containsKey(hostIp)) {
				CleanerTask cleanerTask = new CleanerTask(hostIp, jobId, reduceTaskIds.size());
				cleanerTask.addReducerFile(reduceClean.get(hostIp));
				cleanTaskTbl.put(hostIp, cleanerTask);
			} else {
				cleanTaskTbl.get(hostIp).addReducerFile(reduceClean.get(hostIp));
			}
		}
		
		/* print out to check */
		System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
		Set<String> hosts = cleanTaskTbl.keySet();
		for (String host : hosts) {
			System.out.println("CleanerTask for TaskTracker " + host);
			CleanerTask task = cleanTaskTbl.get(host);
			/* add tasks to the specific TaskTracker's queue */
			this.jobScheduler.addCleanTask(task);
			System.out.println("Job id = " + task.getJobId() + " partitionNum = " + task.getPartitionNum());
			System.out.println("MapperFileName: " + Arrays.toString(task.getMapperFile().toArray()));
			System.out.println("ReducerFileName: " + Arrays.toString(task.getReducerFile().toArray()));
		}
		System.out.println("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
	}
	
	@Override
	public int checkMapProgress(String jobId) throws RemoteException {
		return this.jobStatusTbl.get(jobId).mapTaskLeft;
	}

	@Override
	public int checkReduceProgress(String jobId) throws RemoteException {
		return this.jobStatusTbl.get(jobId).reduceTaskLeft;
	}
	
	@Override
	public JobStatus getJobStatus(String jobId) throws RemoteException {
		return this.jobStatusTbl.get(jobId);
	}
	
	@Override
	public AbstractMap<String, JobStatus> getAllJobStatus() throws RemoteException {
		return this.jobStatusTbl;
	}
	
	/* JobScheduler */
	private class JobScheduler {
		
		public ConcurrentHashMap<String, Queue<Task>> taskScheduleTbl = new ConcurrentHashMap<String, Queue<Task>>();
	
		
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
					if (workLoad < minLoad && workLoad < JobTracker.this.taskTrackerTbl.get(entry.dataNodeRegistryIP).getNumSlots() 
							&& taskTrackerTbl.get(entry.dataNodeRegistryIP).getStatus() == TaskTrackerInfo.Status.RUNNING) {
						bestIp = entry.dataNodeRegistryIP;
						minLoad = workLoad;
					}
				}
			}
			
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
//			if (Hdfs.DEBUG) {
//				System.out.println("DEBUG JobTracker.Scheduler.addReduceTask(): add map task " + task.getTaskId() + " to TaskTracker " + bestIp + " Queue");
//			}
			taskScheduleTbl.get(bestIp).add(task);
		}

		
		/**
		 * Schedule a reduce task, idle task tracker first
		 * @param task The task to schedule
		 */
		public synchronized void addReduceTask(ReducerTask task) {
			/* pick the one with lightest workload */
			String bestIp = null;
			int minLoad = Integer.MAX_VALUE;
			Set<String> allNodes = taskScheduleTbl.keySet();
			
			for (String ip : allNodes) {
				if (taskTrackerTbl.get(ip).getStatus() == TaskTrackerInfo.Status.RUNNING) {
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
			}
			if (Hdfs.Core.DEBUG) {
				System.out.println("DEBUG JobTracker.Scheduler.addReduceTask(): add reduce task " + task.getTaskId() + " to TaskTracker " + bestIp + " Queue");
			}
			taskScheduleTbl.get(bestIp).add(task); 
		}
		
		/**
		 * Assign clean tasks after a job SUCCESS
		 * @param task
		 */
		public void addCleanTask(CleanerTask task) {
			String taskTrackerIp = task.getTaskTrackerIp();
			taskScheduleTbl.get(taskTrackerIp).add(task);
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
	
	private class TaskTrackerCheck implements Runnable {
		
		@Override
		public void run() {
			while (true) {
//				if (Hdfs.DEBUG) {
//					System.out.println("DEBUG JobTracker.TaskTrackerCheck.run(): TaskTrackerCheck start running");
//				}
				Set<String> taskTrackers = JobTracker.this.taskTrackerTbl.keySet();
				for (String taskTrackerIp : taskTrackers) {
					TaskTrackerInfo taskTrackerInfo = JobTracker.this.taskTrackerTbl.get(taskTrackerIp);
					long lastHeartBeat = taskTrackerInfo.getTimeStamp();
					if (taskTrackerInfo.getStatus() == TaskTrackerInfo.Status.RUNNING 
							&& System.currentTimeMillis() - lastHeartBeat >= MapReduce.JobTracker.TASK_TRACKER_EXPIRATION) {
						if (Hdfs.Core.DEBUG) {
							System.out.println("DEBUG JobTracker.TaskTrackerCheck.run(): TaskTracker " + taskTrackerIp + " not available now, reschedule all relate tasks");
						}
						/* mark the TaskTracker as unavailable so that further tasks
						 * will not be scheduled into its task queue */
						synchronized(taskTrackerInfo) {
							taskTrackerInfo.disable();
						}
						/* re-schedule all tasks previously complete and currently running on this TaskTracker */
						/* A. re-schedule tasks in associative scheduling queue */
						//synchronized(JobTracker.this.jobScheduler.taskScheduleTbl) {
						Queue<Task> tasks = JobTracker.this.jobScheduler.taskScheduleTbl.get(taskTrackerIp);
						synchronized(tasks) {
							for (Task task : tasks) {
								if (Hdfs.Core.DEBUG) {
									System.out.println("DEBUG TaskTrackerCheck.run(): re-schedule task " + task.getTaskId() + " in job " + task.getJobId() + " out of queue");
								}
								if (task instanceof MapperTask) {
									JobTracker.this.jobScheduler.addMapTask((MapperTask) task);
								} else {
									JobTracker.this.jobScheduler.addReduceTask((ReducerTask) task);
								}
							}
						}

						/* clean the queue */
						JobTracker.this.jobScheduler.taskScheduleTbl.put(taskTrackerIp, new PriorityBlockingQueue<Task>(MAX_NUM_MAP_TASK, new SchedulerComparator()));;
						//}
						/* B. re-schedule related tasks on this TaskTracker */
						
						Set<String> taskIds = JobTracker.this.taskTrackerTbl.get(taskTrackerIp).getRelatedTasks();
						/* job id of all re-scheduled mapper tasks, to decide whether to re-schedule the reducer task directly */
						Set<String> jobIds = new HashSet<String>();
						List<String> reducerTaskIds = new LinkedList<String>(); 
						for (String taskId : taskIds) {
							Task taskToSchedule = JobTracker.this.taskTbl.get(taskId);
							JobStatus jobStatus = JobTracker.this.jobStatusTbl.get(taskToSchedule.getJobId());
							if (jobStatus.status != WorkStatus.RUNNING) {
								continue;
							}
							if (taskToSchedule instanceof MapperTask) {
								if (jobStatus.mapperStatusTbl.get(taskId).status
										== WorkStatus.SUCCESS) {
									jobStatus.mapTaskLeft++;
									jobStatus.mapperStatusTbl.get(taskId).status = WorkStatus.RUNNING;
								}
								if (Hdfs.Core.DEBUG) {
									System.out.println("DEBUG TaskTrackerCheck.run(): re-schedule task(map) " + taskId + " in job " + taskToSchedule.getJobId() + " out of tasktracker history");
								}
								JobTracker.this.jobScheduler.addMapTask((MapperTask) taskToSchedule);
								/* renew the reducerStatusTlb, all previous reducers should be discarded
								 * because new reducer tasks will be assigned once mapperLeft count is 
								 * zero */
								jobStatus.reducerStatusTbl = new ConcurrentHashMap<String, TaskStatus>();
								jobIds.add(jobStatus.jobId);
							} else {
								/* if a mapper task of the same job has been re-scheduled, this reducer
								 * does not need to re-schedule, it will be upon all mappers finished */
								reducerTaskIds.add(taskToSchedule.getTaskId());
							}
						}
						for (String reducerId : reducerTaskIds) {
							Task taskToSchedule = JobTracker.this.taskTbl.get(reducerId);
							JobStatus jobStatus = JobTracker.this.jobStatusTbl.get(taskToSchedule.getJobId());
							if (!jobIds.contains(jobStatus.jobId)) {
								if (jobStatus.reducerStatusTbl.get(reducerId).status
										== WorkStatus.RUNNING) {						
									if (Hdfs.Core.DEBUG) {
										System.out.println("DEBUG TaskTrackerCheck.run(): re-schedule task(reduce) " + reducerId + " in job " + taskToSchedule.getJobId() + " out of tasktracker history");
									}
									/* no mapper of this job being re-scheduled in previous step, re-schedule this reducer */
									JobTracker.this.jobScheduler.addReduceTask((ReducerTask) taskToSchedule);
								}
							}
						}
						/* clean task record on this taskTracker's info since all have been re-scheduled */
						JobTracker.this.taskTrackerTbl.get(taskTrackerIp).cleanTasks();
					}
				}
				try {
					Thread.sleep(1000 * 20);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			
		}
	}
}
