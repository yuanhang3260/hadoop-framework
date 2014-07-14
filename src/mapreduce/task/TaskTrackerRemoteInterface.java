package mapreduce.task;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface TaskTrackerRemoteInterface extends Remote {
	public boolean runTask(Task task) throws RemoteException;
}
