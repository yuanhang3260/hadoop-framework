package hdfs.DataStructure;

import java.io.Serializable;

public class DataNodeEntry implements Serializable {

	private static final long serialVersionUID = -4553871129664598137L;
	public String dataNodeRegistryIP;
	public int dataNodeRegistryPort;
	private String dataNodeName;
	
	public DataNodeEntry (String ip, int port, String dataNodeName) {
		this.dataNodeRegistryIP = ip;
		this.dataNodeRegistryPort = port;
		this.dataNodeName = dataNodeName;
	}
	
	public String getNodeName() {
		return this.dataNodeName;
	}
}
