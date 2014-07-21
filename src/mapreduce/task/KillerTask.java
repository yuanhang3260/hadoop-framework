package mapreduce.task;

import java.util.List;

public class KillerTask extends Task {

	private static final long serialVersionUID = -2032059767414904871L;
	
	private String jobId;
	private List<String> taskIds;
	private transient String taskTrackerIp;

	public KillerTask(String ip, String jobId, String tid, List<String> taskIds) {
		super(jobId, tid, 2);
		this.taskTrackerIp = ip;
		this.jobId = jobId;
		this.taskIds = taskIds;
	}
	
	
	public String getJobId() {
		return this.jobId;
	}
	
	public List<String> getTaskIds() {
		return this.taskIds;
	}
	
	public String getTaskTrackerIp() {
		return this.taskTrackerIp;
	}
}
