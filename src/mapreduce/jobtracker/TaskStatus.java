package mapreduce.jobtracker;

import java.io.Serializable;

public class TaskStatus implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 8889285369480650292L;
	public String jobId;
	public String taskId;
	public WorkStatus status;

	/*
	 * indicate information of the task tracker that executed this task, last
	 * turn
	 */
	public String taskTrackerIp;
	public int taskTrackerPort;

	public TaskStatus(String jobId, String taskId, WorkStatus status,
			String ip, int port) {
		this.jobId = jobId;
		this.taskId = taskId;
		this.status = status;
		this.taskTrackerIp = ip;
		this.taskTrackerPort = port;
	}
}

