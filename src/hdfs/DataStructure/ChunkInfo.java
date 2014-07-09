package hdfs.DataStructure;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class ChunkInfo implements Serializable {

	private static final long serialVersionUID = -253895999225595586L;
	private List<DataNodeEntry> dataNodeLocations;
	private String chunkName;

	
	public ChunkInfo(String chunkName) {
		dataNodeLocations = new ArrayList<DataNodeEntry>();
		this.chunkName = chunkName;
	}
	
	public void addDataNode(String ip, int port) {
		DataNodeEntry newDataNode = new DataNodeEntry(ip, port);
		this.dataNodeLocations.add(newDataNode);
		return;
	}
	
	public DataNodeEntry getDataNode(int index) {
		return this.dataNodeLocations.get(index);
	}
	
	
	public int getReplicaFactor() {
		return this.dataNodeLocations.size();
	}
	
	public String getChunkName() {
		return this.chunkName;
	}
	
	public List<DataNodeEntry> getAllLocations() {
		return this.dataNodeLocations;
	}
	
}
