package mapreduce.task;

import java.io.Serializable;

public class JarFileEntry implements Serializable {	
	
	private String taskTrackerIp;
	
	private int serverPort;
	
	public JarFileEntry (String taskTrackerIp, int serverPort) {
		
		this.taskTrackerIp = taskTrackerIp;
		
		this.serverPort = serverPort;
		
	}
	
	public String getTaskTrackerIp() {
		return this.taskTrackerIp;
	}
	
	public int getServerPort() {
		return this.serverPort;
	}

}
