package hdfs.IO;

import java.io.Serializable;

public class HDFSLineFeedCheck implements Serializable {
	
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -1451262205640257069L;
	public String line;
	public boolean metLineFeed;
	
	public HDFSLineFeedCheck(String line, boolean rst) {
		this.line = line;
		this.metLineFeed = rst;
	}
}
