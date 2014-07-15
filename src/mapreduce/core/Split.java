package mapreduce.core;

import java.io.Serializable;

import hdfs.DataStructure.HDFSFile;

public class Split implements Serializable {
	HDFSFile file;
	
	/* for now, take a whole chunk as a split */
	int chunkIdx;
	
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
