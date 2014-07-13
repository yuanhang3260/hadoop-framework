package mapreduce.core;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface TaskTrackerRemoteInterface extends Remote {
	public boolean runTast(Task task) throws RemoteException;
}
