package hdfs.DataNode;

import hdfs.io.HDFSLineFeedCheck;

import java.io.IOException;
import java.rmi.Remote;
import java.rmi.RemoteException;

public interface DataNodeRemoteInterface extends Remote{
	public void writeToLocal(byte[] b, String chunkName, int offset) throws RemoteException;
	public String readChunk(String chunkName) throws RemoteException, IOException;
	public byte[] read(String chunkName, int offSet) throws RemoteException;
	public void commitChunk(String globalChunkName, boolean valid, boolean forSysCheck) throws RemoteException;
	public void deleteChunk(String globalChunkName) throws RemoteException, IOException;
	public HDFSLineFeedCheck readLine(String chunkName) throws RemoteException, IOException;
	public void deleteFirstLine(String chunkName, boolean firstFile) throws RemoteException, IOException;
}
