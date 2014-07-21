package mapreduce.task;

import mapreduce.io.Split;

public class MapperTask extends Task{
	
	private static final long serialVersionUID = -4448997561951975942L;
	public Split split;
	public Class<?> mapperClass;
	private int partitionNum;
	private JarFileEntry jarEntry;
	
	public MapperTask(String jobId, String tid, int level, Split split, Class<?> theClass, int num, JarFileEntry jarEntry) {
		super(jobId, tid, level);
		this.split = split;
		this.mapperClass = theClass;
		this.partitionNum = num;
		this.jarEntry = jarEntry;
	}
	
	/**
	 * Constructor for MapperTask
	 * @param jobId Job ID
	 * @param tid Task ID
	 * @param split The split assigned to mapper
	 * @param theClass The mapper class submitted by client
	 * @param num Total partition number
	 */
	public MapperTask(String jobId, String tid, Split split, Class<?> theClass, int num, JarFileEntry jarEntry) {
		super(jobId, tid, 0);
		this.split = split;
		this.mapperClass = theClass;
		this.partitionNum = num;
		this.jarEntry = jarEntry;
	}
	
	public Split getSplit() {
		return this.split;
	}
	
	public int getPartitionNum() {
		return this.partitionNum;
	}
	
	public JarFileEntry getJarEntry() {
		return this.jarEntry;
	}

}
