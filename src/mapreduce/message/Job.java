package mapreduce.message;

import java.io.Serializable;
import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.List;

import mapreduce.client.JobConf;
import mapreduce.io.Split;

/**
 * An abstraction of a specific job, JobClient submits a job to JobTracker
 *
 */
public class Job implements Serializable{

	private static final long serialVersionUID = -280531931740408893L;
	private String jobId;
	private JarFileEntry jarFileEntry;
	private JobConf conf;
	private List<Split> splits;
	
	public Job(JobConf conf) {
		this.conf = conf;
	}
	
	public void setJarFileEntry(String jarPath) throws UnknownHostException {
		this.jarFileEntry = new JarFileEntry(Inet4Address.getLocalHost().getHostAddress(),  
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
	}
	
	public List<Split> getSplit() {
		return this.splits;
	}
}
