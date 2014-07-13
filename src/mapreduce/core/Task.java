package mapreduce.core;

public class Task {
	String jid;
	String tid;
	//Split split;
	Class<?> taskClass;
	//InputFormat;
	TaskStatus status;
	int partitionNum;
	
	private enum TaskStatus {
		RUNNING, TERMINATED, FAILED;
	}
}
