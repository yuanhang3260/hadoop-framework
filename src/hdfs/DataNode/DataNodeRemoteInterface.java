package hdfs.DataNode;

import java.io.IOException;
import java.rmi.Remote;
import java.rmi.RemoteException;

public interface DataNodeRemoteInterface extends Remote{
	public void writeToLocal(byte[] b, String chunkName, int offset) throws RemoteException;
	public byte[] read(String chunkName, int offSet) throws RemoteException;
	public void commitChunk(String globalChunkName) throws RemoteException;
	public void deleteChunk(String globalChunkName) throws RemoteException, IOException;
}
