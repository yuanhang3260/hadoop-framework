package hdfs.IO;

import global.Hdfs;
import hdfs.DataNode.DataNodeRemoteInterface;
import hdfs.DataStructure.DataNodeEntry;
import hdfs.DataStructure.HDFSChunk;

import java.io.Serializable;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.List;

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
	
	
	public HDFSInputStream(List<HDFSChunk> chunkInfoList) {
		this.fileChunkInfoList = chunkInfoList;
		this.readBuffer = new byte[Hdfs.Client.READ_BUFFER_SIZE];
		/* indicate no data in read buffer yet */
		this.bufferOffSet = Hdfs.Client.READ_BUFFER_SIZE;
		this.chunkOffSet = 0;
		this.chunkCounter = 0;	
		this.endOfChunk = true;
		this.endOfFile = false;
		this.currChunkInfo = null;
		this.dataNodeStub = null;
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
	
	public int read(byte[] b) throws RemoteException {
		if (b == null || b.length == 0) {
			return 0;
		}
		/* empty space left in buffer byte[] b */
		int bytesLeft = b.length;
		DataNodeEntry dataNodeEntry = null;
		Registry dataNodeRegistry = null;
		while (bytesLeft != 0 && (!endOfFile)) {
			if (Hdfs.DEBUG) {
				System.out.println("--->");
				printInfo();
				System.out.println("bytesLeft: " + bytesLeft);
				System.out.println("<---");
			}
			/* Fetch chunk data and fill in readBuf */
			if (bufferOffSet == readBuffer.length) {
				if (Hdfs.DEBUG) {
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
				if (readBuffer.length != Hdfs.Client.READ_BUFFER_SIZE) {
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
				if (Hdfs.DEBUG) {
					System.out.println("DEBUG HDFSInputStream read() CASE 2: read from readBuffer");
				}
				/* still read from this buf into b*/
				int len = Math.min(bytesLeft, readBuffer.length - bufferOffSet);
				System.arraycopy(readBuffer, bufferOffSet, b, b.length - bytesLeft, len);
				bytesLeft -= len;
				bufferOffSet += len;
			}
		}
		
		if (Hdfs.DEBUG) {
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
	
	private DataNodeEntry getNearestDataNode(List<DataNodeEntry> entries) {
		//TODO:find the nearest data node among the list
		return entries.get(0);
	}

}
