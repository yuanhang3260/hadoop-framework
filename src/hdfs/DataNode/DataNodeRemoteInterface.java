package hdfs.DataNode;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface DataNodeRemoteInterface extends Remote{
	public void write(byte[] b, String chunkName, int offset) throws RemoteException;
}
