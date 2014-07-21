package mapreduce.task;


public class ReducerTask extends Task implements MapRedTask {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -9087455093923296482L;
	private int seq;
	private String reducerClassName;
	private PartitionEntry[] partitionEntry;
	private String outputPath;
	private JarFileEntry jarEntry;

	public ReducerTask(String jobId, String tid, int level, int reducerSEQ, String theClassName, PartitionEntry[] partitionEntry, String path, JarFileEntry jarEntry) {
		super(jobId, tid, level);
		this.seq = reducerSEQ;
		this.reducerClassName = theClassName;
		this.partitionEntry = partitionEntry;
		this.outputPath = path;
		this.jarEntry = jarEntry;
	}
	
	public ReducerTask(String jobId, String tid, int reducerSEQ, String theClassName, PartitionEntry[] partitionEntry, String path, JarFileEntry jarEntry) {
		super(jobId, tid, 0);
		this.seq = reducerSEQ;
		this.reducerClassName = theClassName;
		this.partitionEntry = partitionEntry;
		this.outputPath = path;
		this.jarEntry = jarEntry;
	}
	
	public int getSEQ() {
		return this.seq;
	}
	
	public String getReducerClassName() {
		return this.reducerClassName;
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
	
	public void setTaskTrackerLocalJarPath(String localPath) {
		this.jarEntry.setLocalPath(localPath);
	}

	@Override
	public String getJarLocalPath() {
		return this.jarEntry.getLocalPath();
	}

}
