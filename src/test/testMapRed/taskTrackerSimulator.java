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
import mapreduce.task.ReducerTask;
import mapreduce.task.Task;

public class taskTrackerSimulator {
	
	public static boolean randomStatusGen = true;
	public static int taskSuccessRate = 101; //x %
	public static int NUM_OF_HEART_BEAT = 7;
	
	public static void main(String[] args) {
		try {
			Registry jtRegistry = LocateRegistry.getRegistry(MapReduce.Core.JOB_TRACKER_IP, MapReduce.Core.JOB_TRACKER_REGISTRY_PORT);
			JobTrackerRemoteInterface jtStub = (JobTrackerRemoteInterface) jtRegistry.lookup(MapReduce.Core.JOB_TRACKER_SERVICE_NAME);
			jtStub.join("128.237.213.225", 1500, 1200,2); 
			Thread.sleep(1000 * 10);
			TaskTrackerReport report = new TaskTrackerReport(Inet4Address.getLocalHost().getHostAddress(), 5, null);
			JobTrackerACK ack = jtStub.heartBeat(report);
			List<TaskStatus> allStatus;
			
			int reducerCount = 0;
			
			for (int i = 0; i < NUM_OF_HEART_BEAT; i++) {
				System.out.println("DEBUG taskTrackerSimulator.main(): receive tasks from jobTracker:");
				allStatus = new ArrayList<TaskStatus>();
				for (Task task : ack.newAddedTasks) {
					WorkStatus thisStatus;
					if (randomStatusGen) {
						thisStatus = randomStatusGenerator();
					} else {
						thisStatus = WorkStatus.SUCCESS;
						if ( task instanceof ReducerTask && reducerCount < 3) {
							thisStatus = WorkStatus.FAILED;
							reducerCount++;
						}
					}
					TaskStatus status = new TaskStatus(task.getJobId(), task.getTaskId(), thisStatus, Inet4Address.getLocalHost().getHostAddress(), 9999);
					System.out.print("Get task: JobId: " + task.getJobId() + " TaksId: " + task.getTaskId() + " Process result: " + thisStatus);
					if (task instanceof MapperTask) {
						System.out.println(" Map task");
					} else {
						System.out.println(" Reduce task");
					}
					
					allStatus.add(status);
				}
//				if (ack.newAddedTasks.size() > 0) {
//					Thread.sleep(1000 * 60);//make it unavailable
//				}
				
				Thread.sleep(1000 * 3);
				report = new TaskTrackerReport(Inet4Address.getLocalHost().getHostAddress(), 4, allStatus);
				//report = new TaskTrackerReport(Inet4Address.getLocalHost().getHostAddress(), 4, new ArrayList<TaskStatus>());
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
		if (k < taskSuccessRate) {
			return WorkStatus.SUCCESS;
		} else {
			return WorkStatus.FAILED;
		}
	}
}
