package mapreduce.task;

public class PartitionEntry {
	
	private String tid;
	
	private String host;
	private int port;
	
	/**
	 * 
	 * @param tid The mapper's tid which is used to located the produced partition
	 * @param host The TaskTracker's IP on which the mapper runs
	 * @param port The TaskTracker's Server port
	 */
	public PartitionEntry(String tid, String host, int port) {

		this.tid = tid;
		
		this.host = host;
		this.port = port;
	}

	
	public String getTID() {
		return this.tid;
	}
	
	public String getHost() {
		return this.host;
	}
	
	public int getPort() {
		return this.port;
	}
}
