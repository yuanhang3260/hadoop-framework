package hdfs.NameNode;

import hdfs.DataStructure.ChunkInfo;
import hdfs.IO.HDFSInputStream;
import hdfs.IO.HDFSOutputStream;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

public interface NameNodeRemoteInterface extends Remote{
	public void heartBeat(String dataNodeName) throws RemoteException;
	public String join(String ip, int port) throws RemoteException;
	public void chunkReport() throws RemoteException;
	public HDFSOutputStream create(String path) throws RemoteException;
	public HDFSInputStream open(String filePath) throws RemoteException;
	public ChunkInfo applyForNewChunk(String path) throws RemoteException;
	public List<ChunkInfo> getFileChunks(String file) throws RemoteException;
}
