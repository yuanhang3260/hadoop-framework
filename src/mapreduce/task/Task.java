package mapreduce.task;


public class Task {
	String jobId;
	String tid;

	//InputFormat;
	TaskStatus status;
	
	private enum TaskStatus {
		RUNNING, TERMINATED, FAILED;
	}
	
	public Task(String jobId) {
		this.jobId = jobId;
		this.status = TaskStatus.RUNNING;
	}
	
	public String getJobId() {
		return this.jobId;
	}
	
	
	public TaskStatus getTaskStatus() {
		return this.status;
	}
	
}
