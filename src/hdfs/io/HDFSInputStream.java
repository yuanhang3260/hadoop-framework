package hdfs.io;

import global.Hdfs;
import hdfs.datanode.DataNodeRemoteInterface;

import java.io.IOException;
import java.io.Serializable;
import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Provide method to read data from a HDFSFile, the read function is implemented
 * with buffer inside to provide efficiency
 *
 */
public class HDFSInputStream implements Serializable{

	private static final long serialVersionUID = 3091088429237095244L;
	/* chunks of this file */
	private List<HDFSChunk> fileChunkInfoList;

	
	private byte[] readBuffer;
	private int bufferOffSet;
	
	private int chunkCounter;
	private int chunkOffSet;
	private boolean endOfChunk;
	private boolean endOfFile;
	
	private HDFSChunk currChunkInfo;
	private DataNodeRemoteInterface dataNodeStub;
	
	private boolean DEBUG = false;
	
	private long pos;
	
	
	public HDFSInputStream(List<HDFSChunk> chunkInfoList) {
		this.fileChunkInfoList = chunkInfoList;
		this.readBuffer = new byte[Hdfs.Core.READ_BUFF_SIZE];
		/* indicate no data in read buffer yet */
		this.bufferOffSet = Hdfs.Core.READ_BUFF_SIZE;
		this.chunkOffSet = 0;
		this.chunkCounter = 0;	
		this.endOfChunk = true;
		this.endOfFile = false;
		this.currChunkInfo = null;
		this.dataNodeStub = null;
		this.pos = 0;
	}
	
	public void printInfo() {
		System.out.println("fileChunkInfoList: ");
		for (HDFSChunk chunk : fileChunkInfoList) {
			System.out.println("		Name: " + chunk.getChunkName());
		}
		System.out.println("readBuffer: length " + readBuffer.length);
		System.out.println(new String(readBuffer));
		System.out.println("bufferOffSet: " + bufferOffSet);
		System.out.println("chunkOffSet: " + chunkOffSet );
		System.out.println("chunkCounter: " + chunkCounter);
		System.out.println("endOfChunk? " + endOfChunk);
		System.out.println("endOfFile? " + endOfFile);
	}
	
	/**
	 * Read up to numLine lines in a chunk data
	 * @param idx
	 * @param numLine
	 * @return
	 * @throws IOException
	 */
	public String readLinesInChunk(int idx, int numLine) throws IOException {
		int size = fileChunkInfoList.size();
		
		if (idx >= size) {
			/* chunk index out of bound */
			return null;
		}
		HDFSChunk chunkInfo = fileChunkInfoList.get(idx);
		DataNodeEntry nodeEntry = getNearestDataNode(chunkInfo.getAllLocations());
		Registry nodeRegistry = null;
		String data = null;
		
		try {
			nodeRegistry = LocateRegistry.getRegistry(nodeEntry.dataNodeRegistryIP, nodeEntry.dataNodeRegistryPort);
			DataNodeRemoteInterface nodeStub = (DataNodeRemoteInterface) nodeRegistry.lookup(Hdfs.Core.DATA_NODE_SERVICE_NAME);
			data = nodeStub.readLines(chunkInfo.getChunkName(), this.pos, numLine);
			pos += data.getBytes().length;
		} catch (RemoteException e) {
			throw new IOException("Failed to read chunk", e);
		} catch (NotBoundException e) {
			throw new IOException("Unable to reach datanode", e);
		}
		
		return data;
	}
	
	/**
	 * Given a chunk index among this file's chunks, read the whole chunk
	 * @param idx
	 * @return String the whole chunk data
	 * @throws RemoteException 
	 * @throws NotBoundException 
	 * @throws Exception 
	 */
	public String readChunk(int idx) throws IOException {
		int size = fileChunkInfoList.size();
		if (idx >= size) {
			/* chunk index out of bound */
			return null;
		}
		HDFSChunk chunkInfo = fileChunkInfoList.get(idx);
		DataNodeEntry nodeEntry = getNearestDataNode(chunkInfo.getAllLocations());
		Registry nodeRegistry = null;
		String data = null;
		
		try {
			nodeRegistry = LocateRegistry.getRegistry(nodeEntry.dataNodeRegistryIP, nodeEntry.dataNodeRegistryPort);
			DataNodeRemoteInterface nodeStub = (DataNodeRemoteInterface) nodeRegistry.lookup(Hdfs.Core.DATA_NODE_SERVICE_NAME);
			data = nodeStub.readChunk(chunkInfo.getChunkName());
		} catch (RemoteException e) {
			throw new IOException("Failed to read chunk", e);
		} catch (NotBoundException e) {
			throw new IOException("Unable to reach datanode", e);
		}
		
		return data;
	}
	
	/**
	 * Read up to the size of input buffer data into the input buffer from 
	 * the HDFSFile that this stream connects to
	 * @param b The buffer to put data in
	 * @return
	 * @throws IOException
	 */
	public int read(byte[] b) throws IOException {
		if (b == null || b.length == 0) {
			return 0;
		}
		/* empty space left in buffer byte[] b */
		int bytesLeft = b.length;
		DataNodeEntry dataNodeEntry = null;
		Registry dataNodeRegistry = null;
		while (bytesLeft != 0 && (!endOfFile)) {
			if (Hdfs.Core.DEBUG && this.DEBUG) {
				System.out.println("--->");
				printInfo();
				System.out.println("bytesLeft: " + bytesLeft);
				System.out.println("<---");
			}
			/* Fetch chunk data and fill in readBuf */
			if (bufferOffSet == readBuffer.length) {
				if (Hdfs.Core.DEBUG && this.DEBUG) {
					System.out.println("DEBUG HDFSInputStream read() CASE 1: no read buffer/buffer need renew");
				}
				
				if (endOfChunk) {
					if (chunkCounter == fileChunkInfoList.size()) {
						endOfFile = true;
						break;
					}
					currChunkInfo = fileChunkInfoList.get(chunkCounter++);
					dataNodeEntry = getNearestDataNode(currChunkInfo.getAllLocations());
					dataNodeRegistry = LocateRegistry.getRegistry(dataNodeEntry.dataNodeRegistryIP, dataNodeEntry.dataNodeRegistryPort);
					try {
						dataNodeStub = (DataNodeRemoteInterface) dataNodeRegistry.lookup("DataNode");
					} catch (NotBoundException e) {
						e.printStackTrace();
					}
					chunkOffSet = 0;
				}
				
				/* get buffer from data node */
				readBuffer = dataNodeStub.read(currChunkInfo.getChunkName(), chunkOffSet);
				if (readBuffer.length != Hdfs.Core.READ_BUFF_SIZE) {
					if (chunkCounter == fileChunkInfoList.size()) {
						endOfFile = true;
					}
					endOfChunk = true;
				} else {
					endOfChunk = false;
				}
				this.bufferOffSet = 0;
				this.chunkOffSet += readBuffer.length;
			} else if (readBuffer.length - bufferOffSet > 0 ) {
				if (Hdfs.Core.DEBUG && this.DEBUG) {
					System.out.println("DEBUG HDFSInputStream read() CASE 2: read from readBuffer");
				}
				/* still read from this buf into b*/
				int len = Math.min(bytesLeft, readBuffer.length - bufferOffSet);
				System.arraycopy(readBuffer, bufferOffSet, b, b.length - bytesLeft, len);
				bytesLeft -= len;
				bufferOffSet += len;
			}
		}
		
		if (Hdfs.Core.DEBUG && this.DEBUG) {
			System.out.println("--->");
			System.out.println("DEBUG HDFSInputStream read() out of while loop");
			printInfo();
			System.out.println("bytesLeft: " + bytesLeft);
			System.out.println("<---");
		}
		
		if (bytesLeft == 0) {
			return b.length;
		} else {
			/* EOF, read the rest in readBuffer */
			int len = Math.min(bytesLeft, readBuffer.length - bufferOffSet);
			System.arraycopy(readBuffer, bufferOffSet, b, b.length - bytesLeft, len);
			bytesLeft -= len;
			bufferOffSet += len;
			return b.length - bytesLeft;
		}
	}
	
	/**
	 * Get the nearest DataNode from local host
	 * 
	 * @param entries
	 * @return the nearest entry from local host
	 */
	private DataNodeEntry getNearestDataNode(List<DataNodeEntry> entries) {
		DataNodeEntry nearestEntry = null;
		
		DataNodeEntry tmpNearest = null;
		
		Set<String> unreachable = new HashSet<String>();
		
		for (int i = 0; i < 3 || nearestEntry != null; i++) {
			try {
				
				long localIp = ipToLong(Inet4Address.getLocalHost().getHostAddress());
				
				long minDist = Long.MAX_VALUE;
				
				for (DataNodeEntry entry : entries) {
					if (unreachable.contains(entry.dataNodeRegistryIP)) {
						continue;
					}
					long thisIp = ipToLong(entry.dataNodeRegistryIP);
					long dist = Math.abs(localIp-thisIp);
					if (dist < minDist) {
						minDist = dist;
						tmpNearest = entry;
					}
				}
				
				if (Hdfs.Core.DEBUG) {
					System.out.println("DEBUG HDFSInputStream() foudn tempNearest ip " + tmpNearest.dataNodeRegistryIP);
				}
				
				try {
					Registry dataNodeRegistry = LocateRegistry.getRegistry(tmpNearest.dataNodeRegistryIP, tmpNearest.dataNodeRegistryPort);
					if (Hdfs.Core.DEBUG) {
						System.out.println("DEBUG HDFSInputStream() try coonnect datanode " + tmpNearest.dataNodeRegistryIP);
					}
					DataNodeRemoteInterface dataNodeS = (DataNodeRemoteInterface) dataNodeRegistry.lookup(Hdfs.Core.DATA_NODE_SERVICE_NAME);
				} catch (RemoteException e) {
					if (Hdfs.Core.DEBUG) {
						System.out.println("DEBUG HDFSInputStream() found unreachable datanode: " + tmpNearest.dataNodeRegistryIP);
					}
					unreachable.add(tmpNearest.dataNodeRegistryIP);
					e.printStackTrace();
					continue;
				} catch (NotBoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					continue;
				}
				
				nearestEntry = tmpNearest;
				
			} catch (UnknownHostException e) {
				e.printStackTrace();
			}
		}
		
		if (Hdfs.Core.DEBUG) {
			System.out.println("DEBUG HDFSInputStream.getNearest(): nearest ip" + nearestEntry.dataNodeRegistryIP);
		}
		return nearestEntry;
	}
	
	private static long ipToLong(String ip) {
		String newIp = ip.replace(".", "");
		long ipInt = Long.parseLong(newIp);
		return ipInt;
	}

}
