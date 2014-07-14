package mapreduce.task;

import mapreduce.core.Split;

public class Task {
	String jid;
	String tid;
	public Split split;
	public Class<?> taskClass;
	//InputFormat;
	TaskStatus status;
	public int partitionNum;
	
	private enum TaskStatus {
		RUNNING, TERMINATED, FAILED;
	}
}
