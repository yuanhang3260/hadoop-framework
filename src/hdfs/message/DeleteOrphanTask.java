package hdfs.message;

public class DeleteOrphanTask extends Task {

	/**
	 * 
	 */
	private static final long serialVersionUID = -796516399560749016L;
	private String chunkName;
	
	public DeleteOrphanTask(String tid, String chunkName) {
		super(tid);
		this.chunkName = chunkName;
	}
	
	public String getChunkName() {
		return this.chunkName;
	}

}
