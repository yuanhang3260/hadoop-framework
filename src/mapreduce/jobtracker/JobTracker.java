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
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import mapreduce.Job;
import mapreduce.core.Mapper;
import mapreduce.io.Split;
import mapreduce.task.MapperTask;
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
	
	/* keep all tasks wait to be submit to taskTracker*/
	//private ArrayBlockingQueue<Task> taskQueue = new ArrayBlockingQueue<Task>(MAX_NUM_MAP_TASK);
	
	public void init() {
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
			this.jobScheduler.taskTrackerToTasksTbl.put(ip, new PriorityQueue<Task>(MAX_NUM_MAP_TASK, new Comparator<Task>() {
				@Override
				public int compare(Task t1, Task t2) {
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
	
	private MapperTask createMapTask(String jobId, Split split, Class<?> theClass, int partitionNum) {
		MapperTask task = new MapperTask(nameTask(), jobId, split, theClass, partitionNum);
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
			MapperTask task = createMapTask(job.getJobId(), split, job.getJobConf().getMapper(), job.getJobConf().getNumReduceTasks());
			
			if (Hdfs.DEBUG) {
				System.out.println("DEBUG JobTracker.addMapTasks(): now adding task " + task.getTaskId() + " to Task Queue");
			}
			
			this.taskTbl.put(task.getTaskId(), task);
			/* TODO: initiate another thread to add all tasks */
			this.jobScheduler.addMapTask(task);
			/* initialize task status record */
			TaskStatus stat = new TaskStatus(job.getJobId(), task.getTaskId(), WorkStatus.RUNNING, null, -1);
			this.jobStatusTbl.get(job.getJobId()).taskStatusTbl.put(task.getTaskId(), stat);	
		}
		if (Hdfs.DEBUG) {
			System.out.println("DEBUG JobTrakcer.initMapTask(): map tasks initialization finished, current job scheduling queue: ");
			this.jobScheduler.printScheduleTbl();
		}
	}
	
	private void initReduceTasks(Job job) {
		int numOfReducer = job.getJobConf().getNumReduceTasks();
	}
	
	
	@Override
	public List<Task> heartBeat(TaskTrackerReport report) {
		if (Hdfs.DEBUG) {
			System.out.println("DEBUG JobTracker.heartBeat(): Receive TaskTrackerReport from " + report.taskTrackerIp);
		}
		//TODO: leave for parallel
		List<TaskStatus> allStatus = report.taskStatus;
		if (allStatus != null) {
			for (TaskStatus taskStatus : allStatus) {
				/* update taskStatus */
				//TODO: SERVERAL CASES EXIST! HANDLE THEM WHEN YOU HAVE THE SATAUS CLASS!
				
				JobStatus jobStatus = this.jobStatusTbl.get(taskStatus.jobId);
				synchronized (jobStatus) {
					TaskStatus preStatus = jobStatus.taskStatusTbl.get(taskStatus.taskId);
					if (preStatus.status == WorkStatus.SUCCESS) {
						continue;
					} else if (preStatus.status == WorkStatus.RUNNING || preStatus.status == WorkStatus.FAILED) {
						if (taskStatus.status == WorkStatus.SUCCESS) {
							if (Hdfs.DEBUG) {
								System.out.println("DEBUG JobTracker.heartBeat(): task " + taskStatus.taskId + " in job " + taskStatus.jobId + " SUCCESS, update status");
							}
							this.jobStatusTbl.get(taskStatus.jobId).taskStatusTbl.put(taskStatus.taskId, taskStatus);
							if (this.taskTbl.get(taskStatus.taskId) instanceof MapperTask) {
								if (Hdfs.DEBUG) {
									System.out.println("DEBUG JobTracker.heartBeat(): a mapper task, decrease mapper count ");
								}
								/* mapper success, update */
								jobStatus.mapTaskLeft--;
								if (jobStatus.mapTaskLeft == 0) {
									/* schedule all reducer tasks */
									if (Hdfs.DEBUG) {
										System.out.println("DEBUG JobTracker.heartBeat(): Job " + taskStatus.jobId + " all mappers finished, schedule a reducer task");
									}
									
								}
							} else {
								/* reducer sucess */
								jobStatus.reduceTaskLeft--;
							}
						} else if(taskStatus.status == WorkStatus.FAILED) {
							//TODO: re-schedule this task, do not re-schedule twice
						}
						
					} else { // preStatus.status == WorkStatus.TERMINATED
						
					}
					
				}
			}
		}
		
		/* assign a number of tasks back to task tracker */
		Queue<Task> allTasks = this.jobScheduler.taskTrackerToTasksTbl.get(report.taskTrackerIp);
		List<Task> assignment = new ArrayList<Task>();
		if (report.emptySlot > 0 && allTasks.size() != 0) {
			int queueSize = allTasks.size();
			for (int i = 0; i < report.emptySlot && i < queueSize; i++) {
				assignment.add(allTasks.poll());
			}
		}
		return assignment;
	}
	
	
	/* JobScheduler */
	private class JobScheduler {
		
		
		public ConcurrentHashMap<String, Queue<Task>> taskTrackerToTasksTbl = new ConcurrentHashMap<String, Queue<Task>>();
		
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
				if (taskTrackerToTasksTbl.containsKey(entry.dataNodeRegistryIP)) {
					int workLoad = taskTrackerToTasksTbl.get(entry.dataNodeRegistryIP).size();
					if (workLoad < minLoad && workLoad < MapReduce.TaskTracker.MAX_NUM_MAP_TASK 
							&& taskTrackerTbl.get(entry.dataNodeRegistryIP).getStatus() == TaskTrackerInfo.Status.RUNNING) {
						bestIp = entry.dataNodeRegistryIP;
						minLoad = workLoad;
					}
				}
			}
			
			if (bestIp == null) {
				/* all the optimal task trackers are full, pick a nearest one */
				long baseIp = ipToLong(entries.get(0).dataNodeRegistryIP);
				long minDist = Long.MAX_VALUE;
				Set<String> allNodes = taskTrackerToTasksTbl.keySet();
				for (String ip : allNodes) {
					int workLoad = taskTrackerToTasksTbl.get(ip).size();
					if (workLoad < minLoad && workLoad < MapReduce.TaskTracker.MAX_NUM_MAP_TASK 
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
			if (bestIp == null) {
				return;
			}
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
		
		private void printScheduleTbl() {
			Set<String> taskTrackers = this.taskTrackerToTasksTbl.keySet();
			for (String each : taskTrackers) {
				System.out.println(">>>>>>>>>>>>>>>>>>>>>");
				System.out.println("TaskTracker: " + each + " assigned tasks: ");
				Queue<Task> tasks = this.taskTrackerToTasksTbl.get(each);
				for (Task task : tasks) {
					System.out.println("TaskId: " + task.getTaskId() + " jobId: " + task.getJobId() + " class name: " + task.getClass().getName());
				}
				System.out.println("<<<<<<<<<<<<<<<<<<<<<");
			}
		}
	}
	
}
