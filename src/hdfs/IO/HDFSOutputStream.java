package hdfs.IO;

import global.Hdfs;
import hdfs.DataNode.DataNodeRemoteInterface;
import hdfs.NameNode.ChunkInfo;
import hdfs.NameNode.ChunkInfo.DataNodeInfo;
import hdfs.NameNode.NameNodeRemoteInterface;

import java.io.IOException;
import java.io.Serializable;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Arrays;

public class HDFSOutputStream implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -488017724627896978L;
	private String filePath;
	private String nameNodeReigstryIP;
	private int nameNodeRegistryPort;
	private int chunkOffset;
	private int chunkCounter;
	private int chunksize;
	private ChunkInfo currChunk;
	
	public HDFSOutputStream (int size, ChunkInfo chunk, String file, String ip, int port) {
		this.chunksize = size;
		this.currChunk = chunk;
		this.chunkOffset = 0;
		this.chunkCounter = 1;
		this.filePath = file;
		this.nameNodeReigstryIP = ip;
		this.nameNodeRegistryPort = port;
	}

	public void write(byte[] content) throws IOException {
		
		if (this.chunkOffset == this.chunksize && content.length > 0) { //Apply for a new chunk
			try {
				this.currChunk = newChunk();
			} catch (RemoteException e) {
				e.printStackTrace();
				throw new IOException("Unable to locate NameNode");
			} catch (NotBoundException e) {
				e.printStackTrace();
				throw new IOException("Unable to locate NameNode");
			}
			this.chunkOffset = 0;
		}
		
		int availableBytes = this.chunksize - this.chunkOffset;
		System.out.println("availabelBytes = " + availableBytes); 
		int bufferOffset = 0;
		
		while (bufferOffset < content.length) {	
			if (availableBytes + bufferOffset < content.length) { // need to create a new chunk
				byte[] writeToDataNodeBuf = Arrays.copyOfRange(content, bufferOffset, bufferOffset + availableBytes);
				System.out.println("write to tmp-" + chunkCounter + "\tfrom " + bufferOffset + " to " + (bufferOffset + availableBytes));
				bufferOffset += availableBytes;
				for (int i = 0; i < this.currChunk.getReplicaFactor(); i++) {
					System.out.println("DEBUG HDFSOutputStream.write() getReplicaFactor " + this.currChunk.getReplicaFactor());
					writeToDataNode(writeToDataNodeBuf, this.currChunk.getDataNode(i), this.currChunk.getChunkName());
				}
				
				/* Apply for new chunk */
				try {
					this.currChunk = newChunk();
				} catch (RemoteException e) {
					e.printStackTrace();
					throw new IOException("Unable to locate NameNode");
				} catch (NotBoundException e) {
					e.printStackTrace();
					throw new IOException("Unable to locate NameNode");
				}
				this.chunkOffset = 0;
				availableBytes = this.chunksize;
			} else { // enough to finish write
				byte[] writeToDataNodeBuf = Arrays.copyOfRange(content, bufferOffset, content.length);
				System.out.println("[last] write to tmp-" + chunkCounter + "\tfrom " + bufferOffset + " to " + content.length);
				for (int i = 0; i < this.currChunk.getReplicaFactor(); i++) {
					writeToDataNode(writeToDataNodeBuf, this.currChunk.getDataNode(i), this.currChunk.getChunkName());
				}
				this.chunkOffset += writeToDataNodeBuf.length;
				break;
			} 

		}
		System.out.println("finish write. [status] chunkCounter=" + this.chunkCounter + " chunkOffset=" + this.chunkOffset);
	}
	
	private void writeToDataNode(byte[] content, DataNodeInfo dataNode, String chunkName) throws IOException {
		if (Hdfs.DEBUG) {
			System.out.println("DEBUG: HDFSOutputStream: Write to DataNode ip=" + dataNode.dataNodeRegistryIP + ":" + dataNode.dataNodeRegistryPort);
		}
		Registry dataNodeRegistry = LocateRegistry.getRegistry(dataNode.dataNodeRegistryIP, dataNode.dataNodeRegistryPort);
		try {
			DataNodeRemoteInterface dataNodeStub = (DataNodeRemoteInterface) dataNodeRegistry.lookup("DataNode");
			dataNodeStub.write(content, chunkName, this.chunkOffset);
		} catch (NotBoundException e) {
			e.printStackTrace();
		}
	}
	
	private ChunkInfo newChunk() throws RemoteException, NotBoundException {
		Registry nameNodeRegistry = LocateRegistry.getRegistry(this.nameNodeReigstryIP, this.nameNodeRegistryPort);
		NameNodeRemoteInterface nameNodeStub = (NameNodeRemoteInterface) nameNodeRegistry.lookup("NameNode");
		ChunkInfo handler = nameNodeStub.applyForNewChunk(this.filePath);
		return handler;
	}
	
}
