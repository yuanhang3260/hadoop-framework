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
	private JarFileEntry jarEntry;

	public ReducerTask(String jobId, String tid, int level, int reducerSEQ, Class<?> task, PartitionEntry[] partitionEntry, String path, JarFileEntry jarEntry) {
		super(jobId, tid, level);
		this.seq = reducerSEQ;
		this.reducerTask = task;
		this.partitionEntry = partitionEntry;
		this.outputPath = path;
		this.jarEntry = jarEntry;
	}
	
	public ReducerTask(String jobId, String tid, int reducerSEQ, Class<?> task, PartitionEntry[] partitionEntry, String path, JarFileEntry jarEntry) {
		super(jobId, tid, 0);
		this.seq = reducerSEQ;
		this.reducerTask = task;
		this.partitionEntry = partitionEntry;
		this.outputPath = path;
		this.jarEntry = jarEntry;
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
	
	public JarFileEntry getJarEntry() {
		return this.jarEntry;
	}

}
