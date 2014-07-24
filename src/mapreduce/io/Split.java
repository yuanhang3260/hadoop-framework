package mapreduce.io;

import hdfs.io.HDFSFile;

import java.io.Serializable;

/**
 * Represent a piece of data of one HDFSFile. Each mapper task takes one 
 * split of a file and process it. Currently just take the whole chunk
 * of a HDFSFile as a split
 *
 */
public class Split implements Serializable{

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
