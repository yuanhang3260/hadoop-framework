package hdfs.DataNode;

import java.io.IOException;
import java.rmi.Remote;
import java.rmi.RemoteException;

public interface DataNodeRemoteInterface extends Remote{
	public void write(byte[] b, String chunkName, int offset) throws RemoteException;
	public void modifyChunkPermission(String globalChunkName) throws RemoteException;
	public void deleteChunk(String globalChunkName) throws RemoteException, IOException;
}
