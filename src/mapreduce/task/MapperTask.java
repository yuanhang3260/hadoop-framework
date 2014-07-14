package mapreduce.task;

import mapreduce.core.Split;

public class MapperTask extends Task{
	
	public Split split;
	public Class<?> mapperClass;
	public int partitionNum;
	
	public MapperTask(String jobId, Split split, Class<?> mapperClass, int num) {
		super(jobId);
		this.split = split;
		this.mapperClass = mapperClass;
		this.partitionNum = num;
	}
	

}
