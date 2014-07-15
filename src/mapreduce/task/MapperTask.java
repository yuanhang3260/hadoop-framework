package mapreduce.task;

import mapreduce.core.Split;

public class MapperTask extends Task{
	
	public Split split;
	public Class<?> mapperClass;
	public int partitionNum;
	
	public MapperTask(String tid, String jobId, Split split, Class<?> theClass, int num) {
		super(tid, jobId);
		this.split = split;
		this.mapperClass = theClass;
		this.partitionNum = num;
	}
	
	public Split getSplit() {
		return this.split;
	}
	

}
