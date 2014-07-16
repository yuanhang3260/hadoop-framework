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

import mapreduce.jobtracker.JobTrackerRemoteInterface;
import mapreduce.jobtracker.TaskStatus;
import mapreduce.jobtracker.TaskTrackerReport;
import mapreduce.jobtracker.WorkStatus;
import mapreduce.task.Task;

public class taskTrackerSimulator {
	public static void main(String[] args) {
		try {
			Registry jtRegistry = LocateRegistry.getRegistry(MapReduce.JobTracker.jobTrackerRegistryIp, MapReduce.JobTracker.jobTrackerRegistryPort);
			JobTrackerRemoteInterface jtStub = (JobTrackerRemoteInterface) jtRegistry.lookup(MapReduce.JobTracker.jobTrackerServiceName);
			jtStub.join("128.237.213.225", 1500, 4, 4);
			Thread.sleep(1000 * 10);
			TaskTrackerReport report = new TaskTrackerReport(Inet4Address.getLocalHost().getHostAddress(), 4, null);
			List<Task> tasks = jtStub.heartBeat(report);
			List<TaskStatus> allStatus = new ArrayList<TaskStatus>();
			System.out.println("DEBUG taskTrackerSimulator.main(): 1. receive tasks from jobTracker:");
			for (Task task : tasks) {
				System.out.println("JobId: " + task.getJobId() + " TaksId: " + task.getTaskId());
				TaskStatus status = new TaskStatus(task.getJobId(), task.getTaskId(), WorkStatus.SUCCESS, Inet4Address.getLocalHost().getHostAddress(), 9999);
				allStatus.add(status);
			}
			
			Thread.sleep(1000 * 10);
			report = new TaskTrackerReport(Inet4Address.getLocalHost().getHostAddress(), 4, allStatus);
			tasks = jtStub.heartBeat(report);
			
			allStatus = new ArrayList<TaskStatus>();
			System.out.println("DEBUG taskTrackerSimulator.main(): 2. receive tasks from jobTracker:");
			for (Task task : tasks) {
				System.out.println("JobId: " + task.getJobId() + " TaksId: " + task.getTaskId());
				TaskStatus status = new TaskStatus(task.getJobId(), task.getTaskId(), WorkStatus.SUCCESS, Inet4Address.getLocalHost().getHostAddress(), 9999);
				allStatus.add(status);
			}
			
			Thread.sleep(1000 * 10);
			report = new TaskTrackerReport(Inet4Address.getLocalHost().getHostAddress(), 4, allStatus);
			tasks = jtStub.heartBeat(report);
			System.out.println("DEBUG taskTrackerSimulator.main(): 3. receive tasks from jobTracker:");
			for (Task task : tasks) {
				System.out.println("JobId: " + task.getJobId() + " TaksId: " + task.getTaskId());
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
	

}
