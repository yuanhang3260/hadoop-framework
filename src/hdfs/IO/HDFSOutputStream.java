package hdfs.IO;

import hdfs.NameNode.ChunkManipulationHandler;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;

public class HDFSOutputStream {

	private int chunkOffset;
	private int chunkCounter;
	private int chunksize;
	private ChunkManipulationHandler chunkHanlder;
	
	public HDFSOutputStream (int size, ChunkManipulationHandler handler) {
		this.chunksize = size;
		this.chunkHanlder = handler;
		this.chunkOffset = 0;
		this.chunkCounter = 1;
	}
	
	public HDFSOutputStream(int size) {
		this.chunksize = size;
		this.chunkOffset = 0;
		this.chunkCounter = 1;
	}
	
	public void write(byte[] content) throws IOException {
		if (this.chunkOffset == this.chunksize && content.length > 0) {
			//Apply for a new chunk
			this.chunkCounter++;
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
				writeToLocal(writeToDataNodeBuf, "tmp-" + chunkCounter);
				this.chunkCounter++;
				this.chunkOffset = 0;
				availableBytes = this.chunksize;
			} else { // enough to finish write
				byte[] writeToDataNodeBuf = Arrays.copyOfRange(content, bufferOffset, content.length);
				System.out.println("[last] write to tmp-" + chunkCounter + "\tfrom " + bufferOffset + " to " + content.length);
				writeToLocal(writeToDataNodeBuf, "tmp-" + chunkCounter);
				this.chunkOffset += writeToDataNodeBuf.length;
				break;
			} 

		}
		System.out.println("finish write. [status] chunkCounter=" + this.chunkCounter + " chunkOffset=" + this.chunkOffset);
	}
	
//	private void writeToDataNode(byte[] content, DataNodeInfo dataNode) throws IOException {
//		dataNode = this.chunkHanlder.getDataNode(i);
//		Registry dataNodeRegistry = LocateRegistry.getRegistry(dataNode.dataNodeRegistryIP, dataNode.dataNodeRegistryPort);
//		DataNodeRemoteInterface dataNodeStub = (DataNodeRemoteInterface) dataNodeRegistry.lookup("DataNode");
//		//dataNodeStub.write(content);
//	}
//	
	
	private void writeToLocal(byte[] content, String filename) {
		File file = new File(filename);
		if (!file.exists()) {
			try {
				file.createNewFile();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		RandomAccessFile out = null;
		try {
			out = new RandomAccessFile(file, "rws");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			out.seek((long)this.chunkOffset);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		try {
			out.write(content);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			out.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	

}
