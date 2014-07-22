package mapreduce.message;

public interface MapRedTask {
	
	public JarFileEntry getJarEntry();
	public void setTaskTrackerLocalJarPath(String localPath);
	public String getJarLocalPath();
	
}
