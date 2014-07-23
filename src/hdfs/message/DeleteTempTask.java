package hdfs.message;

public class DeleteTempTask extends Task {

	
	private static final long serialVersionUID = 439160601414370959L;
	private String chunkName;
	
	public DeleteTempTask(String tid, String chunkName) {
		super(tid);
		this.chunkName = chunkName;
	}
	
	public String getChunkName() {
		return this.chunkName;
	}
}
