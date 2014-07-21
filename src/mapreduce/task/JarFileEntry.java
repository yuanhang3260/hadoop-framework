package mapreduce.task;

import java.io.Serializable;

public class JarFileEntry implements Serializable {	
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -3616669982439495255L;

	private String taskTrackerIp;
	
	private int serverPort;
	
	private String jarFileFullPath;
	
	private String jarFileLocalPath;
	
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
	
	public void setLocalPath(String localPath) {
		this.jarFileLocalPath = localPath;
	}
	
	public String getLocalPath() {
		return this.jarFileLocalPath;
	}

}
