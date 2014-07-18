package mapreduce.tasktracker;

import example.WordCountMapper;
import example.WordCountReducer;
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
import java.util.ArrayList;
import java.util.List;

import mapreduce.Job;
import mapreduce.io.Split;
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

	private Task task;
	
	public static void main(String[] args) throws RemoteException, AlreadyBoundException, NotBoundException, InterruptedException {
		JobTrackerSimulator jt = new JobTrackerSimulator();
		Registry registry = LocateRegistry.createRegistry(MapReduce.JobTracker.jobTrackerRegistryPort);
		JobTrackerRemoteInterface jtStub = (JobTrackerRemoteInterface) UnicastRemoteObject.exportObject(jt, 0);
		registry.bind(MapReduce.JobTracker.jobTrackerServiceName, jtStub);
		
		String jid = "job001";
		
		String tid1 = "task001";
		String tid2 = "task002";
		String tid3 = "task003";
		String tid4 = "task004";
		String tid5 = "task005";
		String tid6 = "task006";
		
		Registry nameNodeR = LocateRegistry.getRegistry(Hdfs.NameNode.nameNodeRegistryIP, Hdfs.NameNode.nameNodeRegistryPort);
		NameNodeRemoteInterface nameNodeS = (NameNodeRemoteInterface) nameNodeR.lookup(Hdfs.NameNode.nameNodeServiceName);
		HDFSFile file1 = nameNodeS.open(MapReduce.TaskTrackerTest1.fileName);

		
		Split split1 = new Split(file1, 0);
		Split split2 = new Split(file1, 1);
		Split split3 = new Split(file1, 2);
		
		jt.task = new MapperTask(jid, tid1, split1, WordCountMapper.class, 3);
		
		Thread.sleep(1000 * 5);
		jt.task = new MapperTask(jid, tid2, split2, WordCountMapper.class, 3);
		
		Thread.sleep(1000 * 5);
		jt.task = new MapperTask(jid, tid3, split3, WordCountMapper.class, 3);

		Thread.sleep(1000 * 5);
		PartitionEntry[] partitionEntry0 = new PartitionEntry[3];
		partitionEntry0[0] = new PartitionEntry(tid1, "localhost", MapReduce.TaskTracker1.taskTrackerServerPort);
		partitionEntry0[1] = new PartitionEntry(tid2, "localhost", MapReduce.TaskTracker1.taskTrackerServerPort);
		partitionEntry0[2] = new PartitionEntry(tid3, "localhost", MapReduce.TaskTracker1.taskTrackerServerPort);
		jt.task = new ReducerTask(jid, tid4, 0, WordCountReducer.class, partitionEntry0, "output-part1");

		Thread.sleep(1000 * 5);
		PartitionEntry[] partitionEntry1 = new PartitionEntry[3];
		partitionEntry1[0] = new PartitionEntry(tid1, "localhost", MapReduce.TaskTracker1.taskTrackerServerPort);
		partitionEntry1[1] = new PartitionEntry(tid2, "localhost", MapReduce.TaskTracker1.taskTrackerServerPort);
		partitionEntry1[2] = new PartitionEntry(tid3, "localhost", MapReduce.TaskTracker1.taskTrackerServerPort);
		jt.task = new ReducerTask(jid, tid5, 1, WordCountReducer.class, partitionEntry1, "output-part2");
		
		Thread.sleep(1000 * 5);
		PartitionEntry[] partitionEntry2 = new PartitionEntry[3];
		partitionEntry2[0] = new PartitionEntry(tid1, "localhost", MapReduce.TaskTracker1.taskTrackerServerPort);
		partitionEntry2[1] = new PartitionEntry(tid2, "localhost", MapReduce.TaskTracker1.taskTrackerServerPort);
		partitionEntry2[2] = new PartitionEntry(tid3, "localhost", MapReduce.TaskTracker1.taskTrackerServerPort);
		jt.task = new ReducerTask(jid, tid6, 2, WordCountReducer.class, partitionEntry2, "output-part3");
		

	}
	
	@Override
	public String submitJob(Job job) throws RemoteException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String join(String ip, int port, int mapSlots, int reduceSlots)
			throws RemoteException {
		
		this.taskTrackerIp = ip;
		this.taskTrackerRegistryPort = port;
		this.taskTrackerServerPort = MapReduce.TaskTracker1.taskTrackerServerPort; //TODO: change to join argument
		System.out.println("Target task tracker joins.");
		
		return "TaskTracker-001";
	}

	@Override
	public JobTrackerACK heartBeat(TaskTrackerReport report) {

		report.printReport();
		
		JobTrackerACK rst = new JobTrackerACK(null, null);
		rst.newAddedTasks = new ArrayList<Task>();
		rst.rcvTasks = new ArrayList<TaskStatus>();
		
		if (this.task != null) {
			System.out.println("JobTrackerSimu: run a task!");
			rst.newAddedTasks.add(this.task);
			this.task = null;
			
			if (report != null && report.taskStatus != null && report.taskStatus.size() > 0 ) {
				for (TaskStatus rcvTask : report.taskStatus) {
					if (rcvTask.status == WorkStatus.FAILED || rcvTask.status == WorkStatus.SUCCESS) {
						rst.rcvTasks.add(rcvTask);
					}
				}
			}
			
		} else {
			if (report != null && report.taskStatus != null && report.taskStatus.size() > 0 ) {
				for (TaskStatus rcvTask : report.taskStatus) {
					if (rcvTask.status == WorkStatus.FAILED || rcvTask.status == WorkStatus.SUCCESS) {
						rst.rcvTasks.add(rcvTask);
					}
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

}
