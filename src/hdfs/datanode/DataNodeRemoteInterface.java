package hdfs.datanode;

import hdfs.io.HDFSLineFeedCheck;

import java.io.IOException;
import java.rmi.Remote;
import java.rmi.RemoteException;

public interface DataNodeRemoteInterface extends Remote{
	
	public void writeToLocal(byte[] b, String chunkName, int offset) throws RemoteException, IOException;
	
	public String readChunk(String chunkName) throws RemoteException, IOException;
	
	public byte[] read(String chunkName, int offSet) throws RemoteException, IOException;
	
	public String readLines(String chunkName, long pos, int numLine) throws RemoteException, IOException;
	
	/**
	 * This RMI is for NameNode to inform DataNode to commit a File. By committing a file, its chunks are
	 * closed and the permission is changed to read only.
	 * 
	 * @param globalChunkName
	 * @param valid
	 * @param forSysCheck
	 * @throws RemoteException
	 */
	
	public HDFSLineFeedCheck readLine(String chunkName) throws RemoteException, IOException;
	
	public void deleteFirstLine(String chunkName, boolean firstFile) throws RemoteException, IOException;
}
