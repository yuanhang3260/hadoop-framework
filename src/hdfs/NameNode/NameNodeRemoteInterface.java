package hdfs.NameNode;

import hdfs.DataStructure.DataNodeEntry;
import hdfs.DataStructure.HDFSFile;

import java.io.IOException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;

public interface NameNodeRemoteInterface extends Remote{
	public void heartBeat(String dataNodeName) throws RemoteException;

	public String join(String ip, int port, List<String> chunkNameList) throws RemoteException;
	public HDFSFile create(String path) throws RemoteException;
	public HDFSFile open(String filePath) throws RemoteException;

	public void delete(String path) throws RemoteException, IOException;
	public void chunkReport(String dataNodeName, List<String> chunkList)
			throws RemoteException;
	public String nameChunk() throws RemoteException;
	public List<DataNodeEntry> select(int replicaFactor) throws RemoteException;
	public void commitFile(HDFSFile file) throws RemoteException;
	public ArrayList<String> listFiles() throws RemoteException;
}
