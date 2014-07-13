package mapreduce;

public class Job {
	private String jobId;
	private JobConf conf;
	
	public Job(JobConf conf) {
		this.conf = conf;
	}
	
	public void setJobId(String id) {
		this.jobId = id;
	}
	
	public String getJobId() {
		return this.jobId;
	}
	
	public JobConf getJobConf() {
		return this.conf;
	}
}
