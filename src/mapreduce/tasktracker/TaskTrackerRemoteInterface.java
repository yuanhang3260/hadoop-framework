package mapreduce.tasktracker;

import java.rmi.Remote;
import java.rmi.RemoteException;

import mapreduce.task.Task;

public interface TaskTrackerRemoteInterface extends Remote {
	
	public Task getTask(String taskID) throws RemoteException;
	public boolean toFail() throws RemoteException;
}
