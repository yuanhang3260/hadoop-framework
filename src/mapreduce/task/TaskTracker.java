package mapreduce.task;

import global.Hdfs;
import global.MapReduce;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

import mapreduce.jobtracker.JobTrackerRemoteInterface;

public class TaskTracker implements TaskTrackerRemoteInterface {
	String name;
	String jobTrackerIp;
	int jobTrackerPort;
	int port;
	
	public TaskTracker(String jobTrackerIp, int jobTrackerPort, int port) {
		this.jobTrackerIp = jobTrackerIp;
		this.jobTrackerPort = jobTrackerPort;
		this.port = port;
		try {
			Registry registry = LocateRegistry.createRegistry(port);
			TaskTrackerRemoteInterface stub = (TaskTrackerRemoteInterface) UnicastRemoteObject.exportObject(this, 0);
			registry.rebind(MapReduce.TaskTracker.taskTrackerServiceName, stub);
			
			Registry jobTrackerRegistry = LocateRegistry.getRegistry(jobTrackerIp, jobTrackerPort);
			JobTrackerRemoteInterface jobTrackerStub = (JobTrackerRemoteInterface) jobTrackerRegistry.lookup(MapReduce.JobTracker.jobTrackerServiceName);
			jobTrackerStub.join(Inet4Address.getLocalHost().getHostAddress(), this.port, 2, 2);
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NotBoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	@Override
	public boolean runTask(Task task) throws RemoteException {
		if (Hdfs.DEBUG) {
			System.out.println("DBUG TaskTracker.runTask() receiving task request from jobTracker, taskId: " + task.getTaskId() );
		}
		File taskFile = new File(String.format("tmp/%s-%s-taskFile", name, task.tid));
		try {
			taskFile.createNewFile();
			FileOutputStream fw = new FileOutputStream(taskFile);
			ObjectOutputStream out = new ObjectOutputStream(fw);
			out.writeObject(task);
			out.close();
		} catch (IOException e) {
			return false;
		}
		
		return true;
	}

	@Override
	public void transferPartition(String fileName) throws RemoteException {
		// TODO Auto-generated method stub
		
	}
	
}
