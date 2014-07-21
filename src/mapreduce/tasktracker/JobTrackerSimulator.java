package mapreduce.tasktracker;

import example.WordCount.WordCountMapper;
import example.WordCount.WordCountReducer;
import global.Hdfs;
import global.MapReduce;
import hdfs.DataStructure.HDFSFile;
import hdfs.NameNode.NameNodeRemoteInterface;

import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import mapreduce.Job;
import mapreduce.io.Split;
import mapreduce.jobtracker.JobStatus;
import mapreduce.jobtracker.JobTrackerACK;
import mapreduce.jobtracker.JobTrackerRemoteInterface;
import mapreduce.jobtracker.TaskStatus;
import mapreduce.jobtracker.TaskTrackerReport;
import mapreduce.jobtracker.WorkStatus;
import mapreduce.task.MapperTask;
import mapreduce.task.PartitionEntry;
import mapreduce.task.ReducerTask;
import mapreduce.task.Task;

public class JobTrackerSimulator implements JobTrackerRemoteInterface {
	
	private String taskTrackerIp;
	private int taskTrackerRegistryPort;
	private int taskTrackerServerPort;
	private int counter = 0;

	private List<Task> taskList = Collections.synchronizedList(new LinkedList<Task>());
	
	public static void main(String[] args) throws RemoteException, AlreadyBoundException, NotBoundException, InterruptedException {
		
		JobTrackerSimulator jt = new JobTrackerSimulator();
		
		Registry registry = LocateRegistry.createRegistry(MapReduce.Core.JOB_TRACKER_REGISTRY_PORT);
		JobTrackerRemoteInterface jtStub = (JobTrackerRemoteInterface) UnicastRemoteObject.exportObject(jt, 0);
		registry.bind(MapReduce.Core.JOB_TRACKER_SERVICE_NAME, jtStub);
		
		int chunksNum = 8;
		int reducerNum = 1;
		
		
		String jid = "job001";
		
		String tidPrefix = "task";
		int taskCounter = 1;
		
		/* Open file */
		Registry nameNodeR = LocateRegistry.getRegistry(Hdfs.Core.NAME_NODE_IP, Hdfs.Core.NAME_NODE_REGISTRY_PORT);
		NameNodeRemoteInterface nameNodeS = (NameNodeRemoteInterface) nameNodeR.lookup(Hdfs.Core.NAME_NODE_SERVICE_NAME);
		HDFSFile file1 = nameNodeS.open("hello");
		
		Thread.sleep(1000 * 3);
		
		for (int mapperSEQ = 0; mapperSEQ < chunksNum; mapperSEQ++) {
			Split split = new Split(file1, mapperSEQ);
			jt.taskList.add(new MapperTask(jid, String.format("%s%03d", tidPrefix, taskCounter++), split, WordCountMapper.class, reducerNum));
		}

		Thread.sleep(1000 * 15);
		
		for (int reducerSEQ = 0; reducerSEQ < reducerNum; reducerSEQ++) {
			PartitionEntry[] partitionEntry = new PartitionEntry[chunksNum];
			for (int i = 0; i < chunksNum; i++) {
				partitionEntry[i] = new PartitionEntry(String.format("%s%03d", tidPrefix, (i+1)), "localhost", MapReduce.TaskTracker.Individual.TASK_TRACKER_SERVER_PORT);
				System.err.println(String.format("%s%03d", tidPrefix, (i+1)));
			}
			jt.taskList.add(new ReducerTask(jid, String.format("%s%03d", tidPrefix, taskCounter++), reducerSEQ, WordCountReducer.class, partitionEntry, "output-part" + reducerSEQ));
		}	

	}
	
	@Override
	public String submitJob(Job job) throws RemoteException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String join(String ip, int port, int serverPort, int mapSlots)
			throws RemoteException {
		
		this.taskTrackerIp = ip;
		this.taskTrackerRegistryPort = port;
		this.taskTrackerServerPort = MapReduce.TaskTracker.Individual.TASK_TRACKER_SERVER_PORT; //TODO: change to join argument
		System.out.println("Target task tracker joins.");
		
		return "TaskTracker-001";
	}

	@Override
	public JobTrackerACK heartBeat(TaskTrackerReport report) {

		report.printReport();
		
		JobTrackerACK rst = new JobTrackerACK(null, null);
		rst.newAddedTasks = new ArrayList<Task>();
		rst.rcvTasks = new ArrayList<TaskStatus>();
		
		while (this.taskList.size() > 0) {
			System.out.println("JobTrackerSimu: run a task!");
			synchronized (this.taskList) {
				rst.newAddedTasks.add(this.taskList.get(0));
				this.taskList.remove(0);
			}
		}
			
		if (report != null && report.taskStatus != null && report.taskStatus.size() > 0 ) {
			for (TaskStatus rcvTask : report.taskStatus) {
				if (rcvTask.status == WorkStatus.FAILED || rcvTask.status == WorkStatus.SUCCESS) {
					rst.rcvTasks.add(rcvTask);
				}
			}
		}
			
		return rst;
	}

	@Override
	public int checkMapProgress(String jobId) throws RemoteException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int checkReduceProgress(String jobId) throws RemoteException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public JobStatus getJobStatus(String jobId) throws RemoteException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public AbstractMap<String, JobStatus> getAllJobStatus()
			throws RemoteException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void terminateJob(String jobId) throws RemoteException {
		// TODO Auto-generated method stub
		
	}

}
