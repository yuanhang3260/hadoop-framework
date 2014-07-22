package mapreduce.tasktracker;

import java.rmi.Remote;
import java.rmi.RemoteException;

import mapreduce.message.Task;

public interface TaskTrackerRemoteInterface extends Remote {
	/**
	 * This RMI is used by RunMapper or RunReducer to load task
	 * @param jid Job ID
	 * @param tid Task ID
	 * @return Task the task with exact the same Job ID and Task ID
	 * @throws RemoteException
	 */
	public Task getTask(String jid, String tid) throws RemoteException;

}
