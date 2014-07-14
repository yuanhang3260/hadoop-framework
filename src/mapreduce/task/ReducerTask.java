package mapreduce.task;


public class ReducerTask extends Task {
	
	private int seq;
	private Class<?> reducerTask;
	private PartitionEntry[] partitionEntry;
	private String outputPath;

	public ReducerTask(String jobId, int reducerSEQ, Class<?> task, PartitionEntry[] partitionEntry, String path) {
		super(jobId);
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
