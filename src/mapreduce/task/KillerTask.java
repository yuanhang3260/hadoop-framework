package mapreduce.task;

import java.util.LinkedList;
import java.util.List;

public class KillerTask extends Task {

	private static final long serialVersionUID = -2032059767414904871L;
	
	private String jobId;
	private List<String> taskIds;

	public KillerTask(String jobId, String tid, int level) {
		super(jobId, tid, 2);
		this.jobId = jobId;
		this.taskIds = new LinkedList<String>();
	}
	
	public String getJobId() {
		return this.jobId;
	}
	
	public List<String> getTaskIds() {
		return this.taskIds;
	}
}
