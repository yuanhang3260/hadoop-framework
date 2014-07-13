package mapreduce;

import global.Hdfs;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class JobClient {
	public static void runJob(JobConf conf) {
		try {
			Registry jobTrackerRegistry = LocateRegistry.getRegistry(Hdfs.JobTracker.jobTrackerRegistryIp, Hdfs.JobTracker.jobTrackerRegistryPort);	
			JobTrackerRemoteInterface jobTrackerStub = (JobTrackerRemoteInterface) jobTrackerRegistry.lookup(Hdfs.JobTracker.jobTrackerServiceName);
			Job jobToSubmit = new Job(conf);
			String jobId = jobTrackerStub.submitJob(jobToSubmit);
			if (Hdfs.DEBUG) {
				System.out.println("DEBUG JobClient.runJob(): Job already submitted, job Id = " + jobId);
			}
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NotBoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
}
