package mapreduce.message;

import java.io.InputStream;
import java.io.Serializable;

import mapreduce.jobtracker.WorkStatus;


public class Task implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -6752330217052972746L;
	private String jobId;
	private String tid;
	transient int priorityLevel;
	
	private transient Process procRef;
	private transient InputStream ErrorInputStream;
	private transient InputStream inputStream;
	private int bindProcTimes = 0;
	private String filePrefix;
	
	/* indicate the number of times that this task has been re-scheduled after failure */
	transient int rescheduleNum;
	WorkStatus status;
	
	public Task(String jobId, String tid, int level) {
		this.tid = tid;
		this.jobId = jobId;
		this.priorityLevel = level;
		this.status = WorkStatus.READY;
	}
	
	public int getRescheduleNum() {
		return this.rescheduleNum;
	}
	
	public void increaseRescheuleNum() {
		this.rescheduleNum++;
	}
	
	public void increasePriority() {
		this.priorityLevel++;
	}
	
	public int getPriority() {
		return this.priorityLevel;
	}
	
	
	public String getTaskId() {
		return this.tid;
	}
	
	public String getJobId() {
		return this.jobId;
	}
	
	public WorkStatus getTaskStatus() {
		return this.status;
	}
	
	public void setFilePrefix(String prefix) {
		this.filePrefix = prefix;
	}
	
	public void bindProc (Process p) {
		if (this.bindProcTimes != 0) {
			return;
		}
		this.procRef = p;
		this.bindProcTimes++;
	}
	
	public Process getProcRef() {
		return this.procRef;
	}
	
	public void setProcRef(Process p) {
		this.procRef = p;
		this.ErrorInputStream = p.getErrorStream();
		this.inputStream = p.getInputStream();
	}
	
	public InputStream getErrInputStream() {
		return this.ErrorInputStream;
	}
	
	public InputStream getInputStream() {
		return this.inputStream;
	}
	

		
	/*--------- Task status update ----------*/
	public void startTask() {
		this.status = WorkStatus.RUNNING;
	}
	
	public void failedTask() {
		this.status = WorkStatus.FAILED;
	}
	
	public void commitTask() {
		this.status = WorkStatus.SUCCESS;
	}
	
	/*--------- Task status check ----------*/
	public boolean isRunning() {
		return (this.status == WorkStatus.RUNNING);
	}
	
	public boolean isNOTRunning() {
		return (this.status == WorkStatus.FAILED ||
				this.status == WorkStatus.SUCCESS ||
				this.status == WorkStatus.TERMINATED ||
				this.status == WorkStatus.READY);
	}
	
	/*--------- File Name -------------------*/
	public String localFileNameWrapper(int partitionSEQ) {
		return String.format("%s/%s-%s-%d", this.filePrefix, this.jobId, this.tid, partitionSEQ);
	}
	
	public String remoteFileNameWrapper(int partitionSEQ, String mapperTid) {
		return String.format("%s-%s-%d", this.jobId, mapperTid, partitionSEQ);
	}
	
	public String localReducerFileNameWrapper(String mapperTid) {
		return String.format("%s/%s-%s-%s", this.filePrefix, this.jobId, this.tid, mapperTid);
	}
	
	public void printInfo() {
		System.out.format("task:\t$JID=%s $TID=%s $ProcRef=%s $filePrefix=%s\n", this.jobId, this.tid, (this.procRef != null), this.filePrefix);
	}
	
	public String getFilePrefix() {
		return this.filePrefix;
	}
}
