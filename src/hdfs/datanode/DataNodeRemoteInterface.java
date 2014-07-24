package hdfs.datanode;

import hdfs.io.HDFSLineFeedCheck;

import java.io.IOException;
import java.rmi.Remote;
import java.rmi.RemoteException;

public interface DataNodeRemoteInterface extends Remote{
	
	/**
	 * Provides hdfs client the function to write data to local file system 
	 * via RMI
	 * @param b A buffer with the data to write
	 * @param chunkName The name of the chunk file on local file system
	 * @param offset The offset of the chunk file to write to
	 * @throws RemoteException
	 * @throws IOException
	 */
	public void writeToLocal(byte[] b, String chunkName, int offset) throws RemoteException, IOException;
	
	/**
	 * Assume there's only text data in HDFS, this function reads a whole 
	 * chunk of data into a String
	 * @param chunkName The name of the chunk file to read
	 * @return String a String with all of the data in that chunk
	 * @throws RemoteException
	 * @throws IOException
	 */
	public String readChunk(String chunkName) throws RemoteException, IOException;
	
	/**
	 * A more general function to read chunk file's data starting from a given
	 * offset up to a configured buffer number. 
	 * @param chunkName The name of the chunk file to read
	 * @param offSet The starting offset
	 * @return byte[] array of chunk data, up to a configured buffer size
	 * @throws RemoteException
	 * @throws IOException
	 */
	public byte[] read(String chunkName, int offSet) throws RemoteException, IOException;
	
	/**
	 * Assume all files in DFS are text files. This function provides the 
	 * ability for client to read up to a fixed number of lines' data in
	 * a given chunk file on this DataNode, start from a given position
	 * 
	 * @param chunkName The name of the chunk file
	 * @param pos The starting position
	 * @param numLine The number of lines to read from the chunk file
	 * @return String all the data read from chunk file
	 * @throws RemoteException
	 * @throws IOException
	 */
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
