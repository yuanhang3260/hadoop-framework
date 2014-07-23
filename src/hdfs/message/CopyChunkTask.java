package hdfs.message;

public class CopyChunkTask extends Task {

	/**
	 * 
	 */
	private static final long serialVersionUID = -8879563816291392296L;
	private String chunkName;
	private String srcDataNodeIp;
	private String fileName;
	private int srcDataNodeServerPort;
	
	public CopyChunkTask(String tid, String chunkName, String srcDataNodeIp, int srcDataNodeServerPort, String fileName) {
		super(tid);
		this.chunkName = chunkName;
		this.srcDataNodeIp = srcDataNodeIp;
		this.srcDataNodeServerPort = srcDataNodeServerPort;
		this.fileName = fileName;
		
	}
	
	public String getChunkName() {
		return this.chunkName;
	}
	
	public String getSrcDataNodeIp() {
		return this.srcDataNodeIp;
	}
	
	public String getFileName() {
		return this.fileName;
	}
	
	public int getSrcDataNodeServerPort() {
		return this.srcDataNodeServerPort;
	}

}
