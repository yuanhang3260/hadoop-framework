package mapreduce.task;

import java.util.LinkedList;
import java.util.List;

public class CleanerTask extends Task {

	private static final long serialVersionUID = 2967629977082519094L;
	private String taskTrackerIp;
	private int partitionNum;
	private List<String> mapperFileName;
	private List<String> reducerFileName;
	
	public CleanerTask(String ip, String jobId, String taskId, int partitionNum) {
		super(jobId, taskId, 3);
		this.taskTrackerIp = ip;
		this.partitionNum = partitionNum;
		this.mapperFileName = new LinkedList<String>();
		this.reducerFileName = new LinkedList<String>();
	}
	
	public String getTaskTrackerIp() {
		return this.taskTrackerIp;
	}
	
	public int getPartitionNum() {
		return this.partitionNum;
	}
	
	public List<String> getMapperFile() {
		return this.mapperFileName;
	}
	
	public List<String> getReducerFile() {
		return this.reducerFileName;
	}
	
	public void addMapperFile(List<String> list) {
		this.mapperFileName.addAll(list);
	}
	
	public void addReducerFile(List<String> list) {
		this.reducerFileName.addAll(list);
	}
}
