package mapreduce.io;

import hdfs.io.HDFSFile;

import java.io.Serializable;

public class Split implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 9178405296046575926L;

	public HDFSFile file;
	
	/* for now, take a whole chunk as a split */
	public int chunkIdx;
	
	public Split(HDFSFile file, int idx) {
		this.file = file;
		this.chunkIdx = idx;
	}
	
	public HDFSFile getFile() {
		return this.file;
	}
	
	public int getChunkIdx() {
		return this.chunkIdx;
	}
}
