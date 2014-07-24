package mapreduce.message;

import java.io.Serializable;

public class PartitionEntry implements Serializable {

	private static final long serialVersionUID = -3630399948649412039L;

	private String tid;
	
	private String host;
	
	private int serverPort;
	
	/**
	 * 
	 * @param tid The mapper's tid which is used to located the produced partition
	 * @param host The TaskTracker's IP on which the mapper runs
	 * @param serverPort The TaskTracker's Server port
	 */
	public PartitionEntry(String tid, String host, int serverPort) {

		this.tid = tid;
		
		this.host = host;
		this.serverPort = serverPort;
	}

	
	public String getTID() {
		return this.tid;
	}
	
	public String getHost() {
		return this.host;
	}
	
	public int getPort() {
		return this.serverPort;
	}
}
