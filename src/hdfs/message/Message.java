package hdfs.message;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Message implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -8813886744764292033L;
	private List<Task> taskList;
	
	public Message() {
		this.taskList = new ArrayList<Task>();
	}
	
	public void addTask(Task e) {
		this.taskList.add(e);
	}
	
	public List<Task> getTaskList() {
		return this.taskList;
	}
	
}
