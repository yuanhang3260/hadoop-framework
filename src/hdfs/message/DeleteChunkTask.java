package hdfs.message;

public class DeleteChunkTask extends Task {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 9117359058802268355L;
	private String chunkName;
	private String fileName;
	
	/**
	 * @param tid The task ID assigned by the system.
	 * @param chunkName The chunk name to be deleted.
	 */
	public DeleteChunkTask(String tid, String chunkName, String fileName) {
		
		super(tid);
		this.chunkName = chunkName;
		this.fileName = fileName;
		
	}
	
	public String getChunkName() {
		return this.chunkName;
	}
	
	public String getFileName() {
		return this.fileName;
	}
	
	
}
