package mapreduce.tasktracker;

import global.MapReduce;
import global.Parser;

import java.io.IOException;
import java.net.UnknownHostException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;

public class runTaskTracker {
	public static void main(String[] args) {
		
		int tasktrackerSEQ;
		
		if (args == null || args.length < 1) {
			System.out.println("Usage:\t<TaskTracker Sequential>");
			System.exit(1);
		}
		
		tasktrackerSEQ = Integer.parseInt(args[0]);
		
		try {
			Parser.hdfsCoreConf();
			Parser.mapreduceCoreConf();
			Parser.mapreduceTaskTrackerCommonConf();
			Parser.mapreduceTaskTrackerIndividualConf(tasktrackerSEQ);
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("The TaskTracker rountine cannot read configuration info.\n"
					+ "Please confirm the mapreduce.xml is placed as ./conf/mapreduce.xml.\n"
					+ "The TaskTracker routine is shutting down...");
			System.exit(2);
		}
		
		TaskTracker tt = 
			new TaskTracker(
				/*MapReduce.JobTracker.jobTrackerRegistryIp*/
				MapReduce.Core.JOB_TRACKER_IP, 
				MapReduce.Core.JOB_TRACKER_REGISTRY_PORT, 
				MapReduce.TaskTracker.Individual.TASK_TRACKER_REGISTRY_PORT,
				MapReduce.TaskTracker.Individual.TASK_TRACKER_SERVER_PORT,
				MapReduce.TaskTracker.Common.TEMP_FILE_DIR);
		try {
			tt.init();
		} catch (RemoteException e) {
			if (MapReduce.Core.DEBUG) {
				e.printStackTrace();
			}
			System.err.println("Failed to create registry. TaskTracker is shutting down...");
			System.exit(1);
		} catch (UnknownHostException e) {
			if (MapReduce.Core.DEBUG) {
				e.printStackTrace();
			}
			System.err.println("The Internet is unacessible. TaskTracker is shutting down...");
			System.exit(2);
			
		} catch (NotBoundException e) {
			if (MapReduce.Core.DEBUG) {
				e.printStackTrace();
			}
			System.err.println("Cannot find the JobTracker. TaskTracker is shutting down...");
			System.exit(3);
		} catch (IOException e) {
			if (MapReduce.Core.DEBUG) {
				e.printStackTrace();
			}
			System.err.println("IOException. TaskTracker is shutting down...");
			System.exit(4);
		}
	}
}
