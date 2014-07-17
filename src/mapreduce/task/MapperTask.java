package mapreduce.task;

import mapreduce.io.Split;

public class MapperTask extends Task{
	
	public Split split;
	public Class<?> mapperClass;
	public int partitionNum;
	
	public MapperTask(String tid, String jobId, int level, Split split, Class<?> theClass, int num) {
		super(tid, jobId, level);
		this.split = split;
		this.mapperClass = theClass;
		this.partitionNum = num;
	}
	
	public MapperTask(String tid, String jobId, Split split, Class<?> theClass, int num) {
		super(tid, jobId, 0);
		this.split = split;
		this.mapperClass = theClass;
		this.partitionNum = num;
	}
	
	public Split getSplit() {
		return this.split;
	}
	

}
