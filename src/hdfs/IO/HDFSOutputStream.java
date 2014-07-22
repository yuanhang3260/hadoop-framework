package hdfs.io;

import global.Hdfs;
import hdfs.DataNode.DataNodeRemoteInterface;
import hdfs.DataStructure.DataNodeEntry;
import hdfs.DataStructure.HDFSChunk;
import hdfs.DataStructure.HDFSFile;
import hdfs.NameNode.NameNodeRemoteInterface;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.rmi.NotBoundException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Arrays;
import java.util.List;

public class HDFSOutputStream extends OutputStream implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7034464668035678312L;
	private HDFSFile file;
	private int chunk_size;
	private NameNodeRemoteInterface nameNodeStub;
	
	private int chunk_offset = 0;
	private boolean DEBUG = false;
	
	public HDFSOutputStream (HDFSFile file, NameNodeRemoteInterface nameNodeStub) {
		this.file = file;
		this.nameNodeStub = nameNodeStub;
		this.chunk_size = Hdfs.Core.CHUNK_SIZE;
		this.file.addChunk();
	}
	
	@Override
	public void write(int arg0) throws IOException {
		if (this.chunk_offset < this.chunk_size) {
			if (Hdfs.Core.DEBUG && this.DEBUG) {
				System.out.format("[continue %s]: %s\n", getCurrentChunk().getChunkName(), arg0);
			}
			for (int i = 0; i < getCurrentChunk().getReplicaFactor(); i++) {
				writeToDataNode(new byte[] {(byte) arg0}, getCurrentChunk().getDataNode(i), getCurrentChunk().getChunkName());
			}
			this.chunk_offset++;
		} else {
			this.file.addChunk();
			this.chunk_offset = (this.chunk_offset++) % this.chunk_size;
			if (Hdfs.Core.DEBUG && this.DEBUG) {
				System.out.format("[new chunk %s]: %s\n", getCurrentChunk().getChunkName(), arg0);
			}
			for (int i = 0; i < getCurrentChunk().getReplicaFactor(); i++) {
				writeToDataNode(new byte[] {(byte) arg0}, getCurrentChunk().getDataNode(i), getCurrentChunk().getChunkName());
			}
			this.chunk_offset++;
		}
	}
	
	@Override 
	public void write(byte[] b) throws IOException {
		write(b, 0, b.length);
	}
	
	@Override
	public void write(byte[] b, int offset, int len) throws IOException {
		
		if (offset < 0 || offset > b.length) {
			throw new IOException("Arrays out of bound:" + offset);
		}
		
		if (offset + len > b.length || offset + len < 0) {
			throw new IOException("Arrays out of bound:" + offset + len);
		}
		
		int written = 0;
		
		while (written < len) {
			
			if (this.chunk_offset == this.chunk_size) {
				
				this.file.addChunk();
				this.chunk_offset = this.chunk_offset % this.chunk_size;
				
				if (Hdfs.Core.DEBUG && this.DEBUG) {
					System.out.format("[apply    %s]: buffer <offset = %d>;  "
							+ "chunk<offset = %d>\n", getCurrentChunk().getChunkName(), 
							offset + written, this.chunk_offset);
				}
				
			}
			
			if (len - written < this.chunk_size - this.chunk_offset) { //Needn't to apply for a new chunk
				
				int towrite = len - written;
				
				if (Hdfs.Core.DEBUG && this.DEBUG) {
					System.out.format("[continue %s]: buffer <from %d to  %d>;  "
							+ "chunk<from %d to %d>\n", getCurrentChunk().getChunkName(), 
							offset + written, offset + written + towrite, 
							this.chunk_offset, this.chunk_offset + written + towrite);
				}
				
				for (int i = 0; i < getCurrentChunk().getReplicaFactor(); i++) {
					writeToDataNode(Arrays.copyOfRange(b, offset + written, offset + written + towrite), 
							getCurrentChunk().getDataNode(i), getCurrentChunk().getChunkName());
				}
				
				this.chunk_offset += towrite;
				written += towrite;
				
			} else {//if (len - written > this.chunk_size - this.chunk_offset) { // Need fill out current chunk and apply more
				
				int towrite = this.chunk_size - this.chunk_offset;
				
				if (Hdfs.Core.DEBUG && this.DEBUG) {
					System.out.format("[fill out %s]: buffer <from %d to  %d>;  "
							+ "chunk<from %d to %d>\n", getCurrentChunk().getChunkName(), 
							offset + written, offset + written + towrite, 
							this.chunk_offset, this.chunk_offset + towrite);
				}
				
				for (int i = 0; i < getCurrentChunk().getReplicaFactor(); i++) {
					writeToDataNode(Arrays.copyOfRange(b, offset + written, offset + written + towrite), 
							getCurrentChunk().getDataNode(i), getCurrentChunk().getChunkName());
				}
				
				this.chunk_offset += towrite;
				written += towrite;
			} 
			
		}
		
		if (Hdfs.Core.DEBUG && this.DEBUG) {
			System.out.format("[before return] status: Chunk<total chunks = %d, offset = %d, size = %d>, buff<offset = %d, from = %d, to = %d>\n",
					this.file.getChunkList().size(), this.chunk_offset, this.chunk_size, offset + written, offset, len);
		}
		
	}

	private void writeToDataNode(byte[] b, DataNodeEntry dataNode, String chunkName) throws IOException {
		if (Hdfs.Core.DEBUG && this.DEBUG) {
			System.out.println("DEBUG HDFSNewOutputStream.writeToDataNode:\tDataNode ip=" + dataNode.dataNodeRegistryIP + ":" + dataNode.dataNodeRegistryPort);
		}
		Registry dataNodeRegistry = LocateRegistry.getRegistry(dataNode.dataNodeRegistryIP, dataNode.dataNodeRegistryPort);
		try {
			DataNodeRemoteInterface dataNodeStub = (DataNodeRemoteInterface) dataNodeRegistry.lookup(Hdfs.Core.DATA_NODE_SERVICE_NAME);
			dataNodeStub.writeToLocal(b, chunkName, this.chunk_offset);
		} catch (NotBoundException e) {
			e.printStackTrace();
		}
	}
	
	private HDFSChunk getCurrentChunk() {
		return file.getChunkList().get(file.getChunkList().size() - 1);
	}
	
	public void close() throws IOException {
		
		if (Hdfs.Core.DEBUG && this.DEBUG) {
			System.out.println("DEBUG HDFSNewOutputStream.close():\tBefore sort firstlines, current chunks are:");
			for (HDFSChunk chunk : this.file.getChunkList()) {
				System.out.print("\t" + chunk.getChunkName());
			}
		}
		
		this.file.backupChunkList();
		
		rearrangeChunks();
		
		if (Hdfs.Core.DEBUG && this.DEBUG) {
			System.out.println("\n\nDEBUG HDFSNewOutputStream.close():\tAfter sort firstlines, current chunks are:");
			for (HDFSChunk leftChunk : this.file.getChunkList()) {
				System.out.print("\t" + leftChunk.getChunkName());
			}
		}
		
		cleanupChunks();
		this.nameNodeStub.commitFile(file);
	}
	
	
	private void rearrangeChunks() {
		
		List<HDFSChunk> chunkList = this.file.getChunkList();
		
		for (int i = 0; i < chunkList.size(); i++) {
			
			if (i < chunkList.size()) {
				
				HDFSChunk currChunk = chunkList.get(i);
				
				HDFSLineFeedCheck nextFirstLine;
				
				try {
					do {
						if (i + 1 >= chunkList.size()) {
							break;
						}
						HDFSChunk nextChunk = chunkList.get(i + 1);
						DataNodeEntry nextChunkEntry = nextChunk.getDataNode(0);
						
						/* Read "first line" of next chunk */
						Registry nextChunkDataNodeR = LocateRegistry.getRegistry(nextChunkEntry.dataNodeRegistryIP, nextChunkEntry.dataNodeRegistryPort);
						DataNodeRemoteInterface nextChunkDataNodeS = (DataNodeRemoteInterface) nextChunkDataNodeR.lookup(Hdfs.Core.DATA_NODE_SERVICE_NAME);
						nextFirstLine = nextChunkDataNodeS.readLine(nextChunk.getChunkName());
						
						/* Write the "first line" of next chunk to current chunk*/
						for (DataNodeEntry currChunkDataNodeEntry : currChunk.getAllLocations()) {
							Registry currChunkDataNodeR = LocateRegistry.getRegistry(currChunkDataNodeEntry.dataNodeRegistryIP, currChunkDataNodeEntry.dataNodeRegistryPort);
							DataNodeRemoteInterface currDataNodeS = (DataNodeRemoteInterface) currChunkDataNodeR.lookup(Hdfs.Core.DATA_NODE_SERVICE_NAME);
//							currDataNodeS.writeToLocal(nextFirstLine.line.getBytes(), currChunk.getChunkName(), currChunk.getChunkSize());
							currDataNodeS.writeToLocal(nextFirstLine.line.getBytes(), currChunk.getChunkName(), currChunk.getChunkSize());
						}
						currChunk.updateChunkSize(nextFirstLine.line.getBytes().length);
						
						
						/* If the next chunk doesn't contain any "\n" 
						 * Remove the next chunk */
						if (!nextFirstLine.metLineFeed) {
							chunkList.remove(i + 1);
						}

						
					} while (!nextFirstLine.metLineFeed && i + 1 < chunkList.size());
	
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			
		}
	}
	
	private void cleanupChunks() {
		
		List<HDFSChunk> chunkList = this.file.getChunkList();
		
		for (int i = 0; i < chunkList.size(); i++) {

			HDFSChunk currChunk = chunkList.get(i);
			/* remove the first line current chunk */
			for (DataNodeEntry currChunkDataNodeEntry : currChunk.getAllLocations()) {
				try {
					Registry currChunkDataNodeR = LocateRegistry.getRegistry(currChunkDataNodeEntry.dataNodeRegistryIP, currChunkDataNodeEntry.dataNodeRegistryPort);
					DataNodeRemoteInterface currDataNodeS = (DataNodeRemoteInterface) currChunkDataNodeR.lookup(Hdfs.Core.DATA_NODE_SERVICE_NAME);
					if (i == 0) {
						currDataNodeS.deleteFirstLine(currChunk.getChunkName(), true);
					} else {
						currDataNodeS.deleteFirstLine(currChunk.getChunkName(), false);
					}
				} catch (Exception e) {
					//TODO: handle exception
				}
			}
			
		}
	}
}
