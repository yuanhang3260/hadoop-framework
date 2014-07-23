package mapreduce.jobtracker;

import java.io.Serializable;
import java.util.AbstractMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The JobStatus class keeps the status of all related variables
 * with this specific job. The JobTracker keeps a table of each
 * job on it, and updates associative information according to
 * report from TaskTrackers
 *
 */
public class JobStatus implements Serializable {

	private static final long serialVersionUID = -1476068839650049825L;
	
	public String jobId;
	
	public String jobName;
	
	public WorkStatus status;
	
	public AbstractMap<String, TaskStatus> mapperStatusTbl;
	
	public AbstractMap<String, TaskStatus> reducerStatusTbl;
	
	public int mapTaskTotal;
	
	public int mapTaskLeft;
	
	public int reduceTaskTotal;
	
	public int reduceTaskLeft;
	
	public int rescheduleNum;
	
	public JobStatus(String jobId, String jobName, int mapNum, int reduceNum) {
		
		this.jobId = jobId;
		
		this.jobName = jobName;
		
		this.mapTaskTotal = mapNum;
		
		this.mapTaskLeft = mapNum;
		
		this.reduceTaskTotal = reduceNum;
		
		this.reduceTaskLeft = reduceNum;
		
		this.mapperStatusTbl = new ConcurrentHashMap<String, TaskStatus>();
		
		this.reducerStatusTbl = new ConcurrentHashMap<String, TaskStatus>();
		
		this.status = WorkStatus.RUNNING;
		
		this.rescheduleNum = 0;
	}
}
