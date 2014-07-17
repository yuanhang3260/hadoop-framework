package mapreduce.tasktracker;

import java.io.IOException;
import java.net.UnknownHostException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;

import global.MapReduce;

public class runTaskTracker {
	public static void main(String[] args) {
		TaskTracker tt = 
			new TaskTracker(
				/*MapReduce.JobTracker.jobTrackerRegistryIp*/"128.237.213.225", 
				MapReduce.JobTracker.jobTrackerRegistryPort, 
				MapReduce.TasktTracker1.taskTrackerPort,
				MapReduce.TasktTracker1.taskTrackerServerPort,
				MapReduce.TaskTracker.TEMP_FILE_DIR);
		try {
			tt.init();
		} catch (RemoteException e) {
			if (MapReduce.DEBUG) {
				e.printStackTrace();
			}
			System.err.println("Failed to create registry. TaskTracker is shutting down...");
			System.exit(1);
		} catch (UnknownHostException e) {
			if (MapReduce.DEBUG) {
				e.printStackTrace();
			}
			System.err.println("The Internet is unacessible. TaskTracker is shutting down...");
			System.exit(2);
			
		} catch (NotBoundException e) {
			if (MapReduce.DEBUG) {
				e.printStackTrace();
			}
			System.err.println("Cannot find the JobTracker. TaskTracker is shutting down...");
			System.exit(3);
		} catch (IOException e) {
			if (MapReduce.DEBUG) {
				e.printStackTrace();
			}
			System.err.println("IOException. TaskTracker is shutting down...");
			System.exit(4);
		}
	}
}
