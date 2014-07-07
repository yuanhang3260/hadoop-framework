package hdfs.NameNode;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class ChunkInfo implements Serializable {

	private static final long serialVersionUID = -253895999225595586L;
	private List<DataNodeInfo> dataNodeList;
	private long chunkName;

	
	public ChunkInfo(long chunkName) {
		dataNodeList = new ArrayList<DataNodeInfo>();
		this.chunkName = chunkName;
	}
	
	public void addDataNode(String ip, int port) {
		DataNodeInfo newDataNode = new DataNodeInfo(ip, port);
		this.dataNodeList.add(newDataNode);
		return;
	}
	
	public DataNodeInfo getDataNode(int index) {
		return this.dataNodeList.get(index);
	}
	
	public class DataNodeInfo implements Serializable {
		/**
		 * 
		 */
		private static final long serialVersionUID = -4553871129664598137L;
		public String dataNodeRegistryIP;
		public int dataNodeRegistryPort;
		
		public DataNodeInfo (String ip, int port) {
			this.dataNodeRegistryIP = ip;
			this.dataNodeRegistryPort = port;
		}
	}
	
	public int getReplicaFactor() {
		return this.dataNodeList.size();
	}
	
	public String getChunkName() {
		return this.chunkName + "";
	}
}
