package hdfs.NameNode;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface NameNodeRemoteInterface extends Remote{
	public void heartBeat(int dataNodeName) throws RemoteException;
	public int join(String ip, int port) throws RemoteException;
	public void blockReport() throws RemoteException;
}
