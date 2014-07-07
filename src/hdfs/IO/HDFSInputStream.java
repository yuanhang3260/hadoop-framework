package hdfs.IO;

import hdfs.NameNode.ChunkInfo;

import java.io.Serializable;
import java.util.List;

public class HDFSInputStream implements Serializable{

	private static final long serialVersionUID = 3091088429237095244L;
	/* chunks of this file */
	private List<ChunkInfo> fileChunksInfo;
	private String nameNodeReigstryIP;
	private int nameNodeRegistryPort;
	
	public HDFSInputStream(List<ChunkInfo> chunks, String ip, int port) {
		this.fileChunksInfo = chunks;
		this.nameNodeReigstryIP = ip;
		this.nameNodeRegistryPort = port;
	}

}
