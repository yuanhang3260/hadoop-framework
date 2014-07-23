package hdfs.namenode;

import hdfs.io.DataNodeEntry;
import hdfs.io.HDFSFile;
import hdfs.message.Message;

import java.io.IOException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;
import java.util.PriorityQueue;

public interface NameNodeRemoteInterface extends Remote{
	
	/**
	 * 	
	 * Heartbeat RMI by which DataNode pings the NameNode periodically.
	 * @param dataNodeName The name of DataNode which is named when DataNode
	 * joins the cluster.
	 * @throws RemoteException
	 */
	public Message heartBeat(String dataNodeName) throws RemoteException;
	
	/**
	 * Join RMI by which DataNode joins joins the HDFS cluster.
	 * @param ip The IP address of DataNode itself.
	 * @param port The port number of DataNode Registry.
	 * @param chunkNameList The names of chunks scanned by DataNode 
	 * at bootstrapping. When recovered from failure, the NameNode 
	 * can instruct DataNode to clean up useless chunks.
	 * @return The DataNode's name given by NameNode.
	 * @throws RemoteException
	 */
	public Message join(String ip, int registryPort, int serverPort, List<String> chunkNameList) throws RemoteException;
	
	/**
	 * Request to create a file on HDFS.
	 * @param path The name of file to be created.
	 * @return HDFS file descriptor.
	 * @throws RemoteException
	 */
	public HDFSFile create(String path) throws RemoteException, IOException;
	
	/**
	 * Open a file on HDFS in order to read a file.
	 * @param filePath The name of file to open
	 * @return HDFS file descriptor. If the file name is used, a
	 * null is returned.
	 * @throws RemoteException
	 */
	public HDFSFile open(String filePath) throws RemoteException;

	/**
	 * Delete the file on HDFS.
	 * @param The name of file to be deleted.
	 * @throws RemoteException
	 * @throws IOException Some chunk cannot be delete at the 
	 * time requested
	 */
	public void delete(String path) throws RemoteException, IOException;
	
	/**
	 * Chunk report RMI by which DataNode notifies the chunks reside on
	 * NameNode.
	 * @param dataNodeName The name of DataNode.
	 * @param chunkList The names of chunks on this DataNode.
	 * @throws RemoteException
	 */
	public Message chunkReport(String dataNodeName, List<String> chunkList)
			throws RemoteException;
	
	/**
	 * Request NameNode to name a new created chunk to guarantee the 
	 * uniqueness of names of each chunk.
	 * @return The name of the new chunk.
	 * @throws RemoteException
	 */
	public String nameChunk() throws RemoteException;
	
	/**
	 * Request NameNode to select DataNodes to store chunks.
	 * @param replicaFactor The replica factor indicating how
	 * many DataNodes needed to be selected.
	 * @return The list of DataNode entries.
	 * @throws RemoteException
	 */
	public List<DataNodeEntry> select(int replicaFactor) throws RemoteException;
	
	/**
	 * The file to be committed on HDFS systems. The client is charge of managing
	 * all file information before the HDFSOutputStream is closed. Upon closing,
	 * the file information is submitted to NameNode and visible.
	 * @param file The file to be committed.
	 * @throws RemoteException
	 */
	public void commitFile(HDFSFile file) throws RemoteException;
	
	/**
	 * List all files on HDFS.
	 * @return
	 * @throws RemoteException
	 */
	public PriorityQueue<String> listFiles() throws RemoteException;
	
	public String listFileTbl() throws RemoteException;
}
