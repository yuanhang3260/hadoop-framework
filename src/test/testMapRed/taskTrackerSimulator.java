package test.testMapRed;

import global.MapReduce;

import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import mapreduce.jobtracker.JobTrackerACK;
import mapreduce.jobtracker.JobTrackerRemoteInterface;
import mapreduce.jobtracker.TaskStatus;
import mapreduce.jobtracker.TaskTrackerReport;
import mapreduce.jobtracker.WorkStatus;
import mapreduce.task.MapperTask;
import mapreduce.task.Task;

public class taskTrackerSimulator {
	public static void main(String[] args) {
		try {
			Registry jtRegistry = LocateRegistry.getRegistry(MapReduce.JobTracker.jobTrackerRegistryIp, MapReduce.JobTracker.jobTrackerRegistryPort);
			JobTrackerRemoteInterface jtStub = (JobTrackerRemoteInterface) jtRegistry.lookup(MapReduce.JobTracker.jobTrackerServiceName);
			jtStub.join("128.237.213.225", 1500, 4, 4);
			Thread.sleep(1000 * 10);
			TaskTrackerReport report = new TaskTrackerReport(Inet4Address.getLocalHost().getHostAddress(), 4, null);
			JobTrackerACK ack = jtStub.heartBeat(report);
			List<TaskStatus> allStatus;
			
			for (int i = 0; i < 20; i++) {
				System.out.println("DEBUG taskTrackerSimulator.main(): receive tasks from jobTracker:");
				allStatus = new ArrayList<TaskStatus>();
				for (Task task : ack.newAddedTasks) {
					WorkStatus thisStatus = randomStatusGenerator();
					TaskStatus status = new TaskStatus(task.getJobId(), task.getTaskId(), thisStatus, Inet4Address.getLocalHost().getHostAddress(), 9999);
					System.out.print("Get task: JobId: " + task.getJobId() + " TaksId: " + task.getTaskId() + " Process result: " + thisStatus);
					if (task instanceof MapperTask) {
						System.out.println(" Map task");
					} else {
						System.out.println(" Reduce task");
						if (i != 4) {
							status.status = WorkStatus.FAILED;
						}
					}
					allStatus.add(status);
				}
				
				Thread.sleep(1000 * 10);
				report = new TaskTrackerReport(Inet4Address.getLocalHost().getHostAddress(), 4, allStatus);
				ack = jtStub.heartBeat(report);
			}
			
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NotBoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static WorkStatus randomStatusGenerator() {
		Random random = new Random();
		int k = random.nextInt(100);
		if (k < 50) {
			return WorkStatus.SUCCESS;
		} else {
			return WorkStatus.FAILED;
		}
	}
}
//package test.testMapRed;
//
//import global.MapReduce;
//
//import java.net.Inet4Address;
//import java.net.UnknownHostException;
//import java.rmi.NotBoundException;
//import java.rmi.RemoteException;
//import java.rmi.registry.LocateRegistry;
//import java.rmi.registry.Registry;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Random;
//
//import mapreduce.jobtracker.JobTrackerRemoteInterface;
//import mapreduce.jobtracker.TaskStatus;
//import mapreduce.jobtracker.TaskTrackerReport;
//import mapreduce.jobtracker.WorkStatus;
//import mapreduce.task.MapperTask;
//import mapreduce.task.Task;
//
//public class taskTrackerSimulator {
//	public static void main(String[] args) {
//		try {
//			Registry jtRegistry = LocateRegistry.getRegistry(MapReduce.JobTracker.jobTrackerRegistryIp, MapReduce.JobTracker.jobTrackerRegistryPort);
//			JobTrackerRemoteInterface jtStub = (JobTrackerRemoteInterface) jtRegistry.lookup(MapReduce.JobTracker.jobTrackerServiceName);
//			jtStub.join("128.237.213.225", 1500, 4, 4);
//			Thread.sleep(1000 * 10);
//			TaskTrackerReport report = new TaskTrackerReport(Inet4Address.getLocalHost().getHostAddress(), 4, null);
//			List<Task> tasks = jtStub.heartBeat(report);
//			List<TaskStatus> allStatus;
//			
//			for (int i = 0; i < 20; i++) {
//				System.out.println("DEBUG taskTrackerSimulator.main(): receive tasks from jobTracker:");
//				allStatus = new ArrayList<TaskStatus>();
//				for (Task task : tasks) {
//					WorkStatus thisStatus = randomStatusGenerator();
//					TaskStatus status = new TaskStatus(task.getJobId(), task.getTaskId(), thisStatus, Inet4Address.getLocalHost().getHostAddress(), 9999);
//					System.out.print("Get task: JobId: " + task.getJobId() + " TaksId: " + task.getTaskId() + " Process result: " + thisStatus);
//					if (task instanceof MapperTask) {
//						System.out.println(" Map task");
//					} else {
//						System.out.println(" Reduce task");
//						if (i != 4) {
//							status.status = WorkStatus.FAILED;
//						}
//					}
//					allStatus.add(status);
//				}
//				
//				Thread.sleep(1000 * 10);
//				report = new TaskTrackerReport(Inet4Address.getLocalHost().getHostAddress(), 4, allStatus);
//				tasks = jtStub.heartBeat(report);
//			}
//			
//		} catch (RemoteException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (NotBoundException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (InterruptedException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (UnknownHostException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//	}
//	
//	public static WorkStatus randomStatusGenerator() {
//		Random random = new Random();
//		int k = random.nextInt(100);
//		if (k < 50) {
//			return WorkStatus.SUCCESS;
//		} else {
//			return WorkStatus.FAILED;
//		}
//	}
//	
//
//}
