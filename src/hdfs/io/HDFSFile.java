package hdfs.io;

import global.Hdfs;
import hdfs.namenode.NameNodeRemoteInterface;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class HDFSFile implements Serializable {


	private static final long serialVersionUID = -6302186159396021997L;
	private String name;
	private List<HDFSChunk> chunkList; //Indicate chunk order
	private List<HDFSChunk> totalChunkList; //Contains all chunks. Some chunks may be excluded after rearrange.
	private int replicaFactor; 
	private boolean available; //TODO: define the semantics
	private HDFSOutputStream newOutputStream = null;
	private NameNodeRemoteInterface nameNodeStub;
	private Date commitTime;

	
	/**
	 * Constructor
	 * @param name
	 */
	public HDFSFile(String name, int rf, NameNodeRemoteInterface stub) {
		this.name = name;
		this.chunkList = new ArrayList<HDFSChunk>();
		this.replicaFactor = rf;
		this.nameNodeStub = stub;
//		addChunk();
	}
	
	/**
	 * Add new HDFS chunk to the file
	 * @param chunkName the name of the chunk
	 * @param locations the data node list to store the chunk
	 * @return
	 */
	public void addChunk() {
		String chunkName = null;
		List<DataNodeEntry> locations = null;
		try {
			chunkName = this.nameNodeStub.nameChunk();
			System.out.println("DEBUG HDFSFile.addChunk(): chunk name:" + chunkName);
			
			locations = this.nameNodeStub.select(this.replicaFactor);
			
			int i = 0;
			System.out.format("DEBUG HDFSFile.addChunk(): Locations\n");
			for (DataNodeEntry entry: locations) {
				System.out.format("\tIP=%s\t%s\n", entry.dataNodeRegistryIP, entry.dataNodeRegistryPort);
			}
		} catch (RemoteException e) {
			if (Hdfs.Core.DEBUG) { e.printStackTrace(); }
		}
		HDFSChunk newChunk = new HDFSChunk(chunkName, locations);
		this.chunkList.add(newChunk);
	}
	
	public void removeChunk(int index) {
		this.chunkList.remove(index);
	}
	
	public void disableFile () {
		this.available = false;
	}
	
	public void enableFile() {
		this.available = true;
	}
	
	public boolean isFileAvailable() {
		return this.available;
	}
	
	public HDFSChunk getChunkByName(String chunkName) {
		for (HDFSChunk chunk : this.chunkList) {
			if (chunk.getChunkName().equals(chunkName)) {
				return chunk;
			}
		}
		return null;
	}
	
	public int getReplicaFactor() {
		return this.replicaFactor;
	}
	
	public List<HDFSChunk> getChunkList() {
		return this.chunkList;
	}
	
	
	public HDFSOutputStream getOutputStream() {
		this.newOutputStream = new HDFSOutputStream(this, this.nameNodeStub);
		return this.newOutputStream;
	}
	
	public String getName() {
		return this.name;
	}
	
	public HDFSInputStream getInputStream() {
		return new HDFSInputStream(this.getChunkList());
	}
	
	@SuppressWarnings("unchecked")
	public void backupChunkList() {
		this.totalChunkList = (List<HDFSChunk>) ((ArrayList<HDFSChunk>) this.chunkList).clone();
	}
	
	public List<HDFSChunk> getTotalChunkList() {
		return this.totalChunkList;
	}
	
	public void setCommitTime(Date time) {
		this.commitTime = time;
	}
	
	public Date getCommitTime() {
		return this.commitTime;
	}
}

