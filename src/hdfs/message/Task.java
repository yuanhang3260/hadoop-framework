package hdfs.message;

import java.io.Serializable;
import java.util.Date;

public class Task implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 8427383144164863802L;
	private String tid;
	private Date sentTime;
	private MessageStatus status;

	
	public Task(String tid) {
		this.tid = tid;
		this.status = MessageStatus.ENQUEUE;
	}
	
	public String getTid() {
		return this.tid;
	}
	
	public void sentTask() {
		this.status = MessageStatus.SENT;
	}
	
	public void ackTask() {
		this.status = MessageStatus.ACK;
	}
	
}
