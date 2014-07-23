package hdfs.message;

public class CommitBackupTask extends Task{

	/**
	 * 
	 */
	private static final long serialVersionUID = -5046848030063774783L;
	private String chunkName;
	
	public CommitBackupTask(String tid, String chunkName) {
		super(tid);
		this.chunkName = chunkName;
	}
	
	public String getChunkName() {
		return this.chunkName;
	}

}
