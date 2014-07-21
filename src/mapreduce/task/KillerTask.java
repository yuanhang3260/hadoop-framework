package mapreduce.task;

import java.util.LinkedList;
import java.util.List;

public class KillerTask extends Task {

	private static final long serialVersionUID = -2032059767414904871L;
	
	List<String> jobIds;
	List<String> taskIds;

	public KillerTask(String jobId, String tid, int level) {
		super(jobId, tid, 2);
		this.jobIds = new LinkedList<String>();
		this.taskIds = new LinkedList<String>();
	}
	
	public List<String> getJobIds() {
		return this.jobIds;
	}
	
	public List<String> getTaskIds() {
		return this.taskIds;
	}
}
