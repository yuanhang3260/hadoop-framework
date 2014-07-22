package mapreduce;

import global.MapReduce;

import java.io.Serializable;
import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.List;

import mapreduce.io.Split;
import mapreduce.message.JarFileEntry;

public class Job implements Serializable{
	private String jobId;
	private JarFileEntry jarFileEntry;
	private JobConf conf;
	private List<Split> splits;
	
	public Job(JobConf conf) {
		this.conf = conf;
	}
	
	public void setJarFileEntry(String jarPath) throws UnknownHostException {
		this.jarFileEntry = new JarFileEntry(Inet4Address.getLocalHost().getHostAddress(), 
				 							 MapReduce.TaskTracker.Individual.TASK_TRACKER_SERVER_PORT, 
				 							 jarPath);
	}
	
	public JarFileEntry getJarFileEntry() {
		return this.jarFileEntry;
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
