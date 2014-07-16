package mapreduce.io;

import java.io.Serializable;

import hdfs.DataStructure.HDFSFile;

public class Split implements Serializable {
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
