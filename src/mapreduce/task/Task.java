package mapreduce.task;

public class Task {
	String jid;
	String tid;
	//Split split;
	public Class<?> taskClass;
	//InputFormat;
	TaskStatus status;
	public int partitionNum;
	
	private enum TaskStatus {
		RUNNING, TERMINATED, FAILED;
	}
}
