package mapreduce.task;

import java.io.Serializable;

public class JarFileEntry implements Serializable {	
	
	private static final long serialVersionUID = 1400214628501192040L;

	private String taskTrackerIp;
	
	private int serverPort;
	
	private String jarFileFullPath;
	
	public JarFileEntry (String taskTrackerIp, int serverPort, String jarFileFullPath) {
		
		this.taskTrackerIp = taskTrackerIp;
		
		this.serverPort = serverPort;
		
		this.jarFileFullPath = jarFileFullPath;
	}
	
	public String getTaskTrackerIp() {
		return this.taskTrackerIp;
	}
	
	public int getServerPort() {
		return this.serverPort;
	}
	
	public String getFullPath() {
		return this.jarFileFullPath;
	}

}
