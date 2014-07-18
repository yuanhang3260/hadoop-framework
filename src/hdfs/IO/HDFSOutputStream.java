package hdfs.IO;

import global.Hdfs;
import hdfs.DataNode.DataNodeRemoteInterface;
import hdfs.DataStructure.DataNodeEntry;
import hdfs.DataStructure.HDFSChunk;
import hdfs.DataStructure.HDFSFile;
import hdfs.NameNode.NameNodeRemoteInterface;

import java.io.IOException;
import java.io.Serializable;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Arrays;
import java.util.List;

public class HDFSOutputStream implements Serializable {

	private static final long serialVersionUID = -488017724627896978L;
	private HDFSFile file;
	private int chunkOffset;
	private int chunkCounter;
	private int chunksize;
	private NameNodeRemoteInterface nameNodeStub;
	private boolean firstWrite = true;

	
	
	public HDFSOutputStream (HDFSFile file, int size, NameNodeRemoteInterface stub) {
		this.file = file;
		this.chunksize = size;
		this.chunkOffset = 0;
		this.chunkCounter = 1;
		this.nameNodeStub = stub;
	}

	public void write(byte[] b) throws IOException {
		
		if (this.chunkOffset == this.chunksize && b.length > 0 || firstWrite) { //Apply for a new chunk
			file.addChunk();
			this.chunkOffset = 0;
		}
		
		if (firstWrite) {
			firstWrite = false;
		}
		
		int availableBytes = this.chunksize - this.chunkOffset;
		if (Hdfs.DEBUG) {
			System.out.println("DEBUG HDFSOutputStream.write(): availabelBytes = " + availableBytes); 
		}
		int bufferOffset = 0;

		
		while (bufferOffset < b.length) {
			
			if (availableBytes + bufferOffset < b.length) { // need to create a new chunk
				byte[] writeToDataNodeBuf = Arrays.copyOfRange(b, bufferOffset, bufferOffset + availableBytes);
				if (Hdfs.DEBUG) {
					System.out.println("DEBUG HDFSOutputStream.write():\twrite to tmp-" + chunkCounter + "\tfrom " + bufferOffset + " to " + (bufferOffset + availableBytes));
				}
				bufferOffset += availableBytes;
				for (int i = 0; i < getCurrentChunk().getReplicaFactor(); i++) {
					writeToDataNode(writeToDataNodeBuf, getCurrentChunk().getDataNode(i), getCurrentChunk().getChunkName());
				}
				this.chunkOffset += availableBytes;
				
				/* Continue writing till new line feed */
				if (writeToDataNodeBuf[writeToDataNodeBuf.length - 1] != '\n') {
					int i = 0;
					while (bufferOffset < b.length && b[bufferOffset] != '\n') {
						i++;
						bufferOffset++;
					}
					if (bufferOffset < b.length && b[bufferOffset] == '\n') {
						i++;
						bufferOffset++;
					}
					byte[] actualWrittenBuf = Arrays.copyOfRange(b, bufferOffset - i, bufferOffset);
					for (int j = 0; j < getCurrentChunk().getReplicaFactor(); j++) {
						writeToDataNode(actualWrittenBuf, getCurrentChunk().getDataNode(j), getCurrentChunk().getChunkName());
					}
				}
				
				/* Apply for a new chunk */
				if (bufferOffset < b.length) {
					file.addChunk();
					this.chunkOffset = 0;
					availableBytes = this.chunksize;
				} else {
					break;
				}
			} else { // enough to finish write
				byte[] writeToDataNodeBuf = Arrays.copyOfRange(b, bufferOffset, b.length);
				if (Hdfs.DEBUG) {
					System.out.println("DEBUG HDFSOutputStream.wirte():\t[last] write to tmp-" + chunkCounter + "\tfrom " + bufferOffset + " to " + b.length);
				}
				for (int i = 0; i < getCurrentChunk().getReplicaFactor(); i++) {
					writeToDataNode(writeToDataNodeBuf, getCurrentChunk().getDataNode(i), getCurrentChunk().getChunkName());
				}
				this.chunkOffset += writeToDataNodeBuf.length;
				break;
			}

		}
		if (Hdfs.DEBUG) {
			System.out.println("DEBUG HDFSOutputStream.write():\tFinish write. [status] chunkCounter=" + this.chunkCounter + " chunkOffset=" + this.chunkOffset);
		}
	}
	
	
	
	public void close() throws IOException{
		this.nameNodeStub.commitFile(file);
	}
	
	private void writeToDataNode(byte[] content, DataNodeEntry dataNode, String chunkName) throws IOException {
		if (Hdfs.DEBUG) {
			System.out.println("DEBUG: HDFSOutputStream: Write to DataNode ip=" + dataNode.dataNodeRegistryIP + ":" + dataNode.dataNodeRegistryPort);
		}
		Registry dataNodeRegistry = LocateRegistry.getRegistry(dataNode.dataNodeRegistryIP, dataNode.dataNodeRegistryPort);
		try {
			DataNodeRemoteInterface dataNodeStub = (DataNodeRemoteInterface) dataNodeRegistry.lookup("DataNode");
			dataNodeStub.writeToLocal(content, chunkName, this.chunkOffset);
		} catch (NotBoundException e) {
			e.printStackTrace();
		}
	}

	
	private HDFSChunk getCurrentChunk() {
		return file.getChunkList().get(file.getChunkList().size() - 1);
	}
	
	
}
