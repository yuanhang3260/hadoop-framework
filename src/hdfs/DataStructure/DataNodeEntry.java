package hdfs.DataStructure;

import java.io.Serializable;

public class DataNodeEntry implements Serializable {

	private static final long serialVersionUID = -4553871129664598137L;
	public String dataNodeRegistryIP;
	public int dataNodeRegistryPort;
	
	public DataNodeEntry (String ip, int port) {
		this.dataNodeRegistryIP = ip;
		this.dataNodeRegistryPort = port;
	}
}
