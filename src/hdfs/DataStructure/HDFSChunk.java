package hdfs.DataStructure;

import java.io.Serializable;
import java.util.List;

public class HDFSChunk implements Serializable {

	private static final long serialVersionUID = -253895999225595586L;
	private String chunkName;
	private String fileName;
	private List<DataNodeEntry> locations;
	private int replicaFactor;
	

	
	public HDFSChunk(String chunkName, List<DataNodeEntry> list) {
		locations = list;
		this.chunkName = chunkName;
	}
	
	public void addDataNode(String ip, int port, String name) {
		DataNodeEntry newDataNode = new DataNodeEntry(ip, port, name);
		this.locations.add(newDataNode);
		return;
	}
	
	public DataNodeEntry getDataNode(int index) {
		return this.locations.get(index);
	}
	
	
	public int getReplicaFactor() {
		return this.locations.size();
	}
	
	public String getChunkName() {
		return this.chunkName;
	}
	
	public List<DataNodeEntry> getAllLocations() {
		return this.locations;
	}
	
}
