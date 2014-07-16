package mapreduce;

import java.io.Serializable;
import java.util.List;

import mapreduce.io.Split;

public class Job implements Serializable{
	private String jobId;
	private JobConf conf;
	private List<Split> splits;
	
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
	
	public void setSplit(List<Split> splits) {
		this.splits = splits;
		/* set the number of mapper & reducer tasks */
		this.conf.setNumMapTasks(splits.size());
		//this.conf.setNumReduceTasks(splits.size());
	}
	
	public List<Split> getSplit() {
		return this.splits;
	}
}
