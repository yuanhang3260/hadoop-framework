package mapreduce.task;



public class ReducerTask extends Task {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -9087455093923296482L;
	private int seq;
	private Class<?> reducerTask;
	private PartitionEntry[] partitionEntry;
	private String outputPath;

	public ReducerTask(String jobId, String tid, int reducerSEQ, Class<?> task, PartitionEntry[] partitionEntry, String path) {
		super(tid, jobId);
		this.seq = reducerSEQ;
		this.reducerTask = task;
		this.partitionEntry = partitionEntry;
		this.outputPath = path;
	}
	
	public int getSEQ() {
		return this.seq;
	}
	
	public Class<?> getTask() {
		return this.reducerTask;
	}
	
	public PartitionEntry[] getEntries() {
		return this.partitionEntry;
	}
	
	public String getOutputPath() {
		return this.outputPath;
	}

}
