package hdfs.datanode;

import global.Hdfs;
import hdfs.io.HDFSLineFeedCheck;
import hdfs.namenode.NameNodeRemoteInterface;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;

public class DataNode implements DataNodeRemoteInterface, Runnable{
	//TODO:Use XML to configure
	private String nameNodeIp;
	private int nameNodePort;
	private String dataNodeName;
	private int dataNodePort;
	private String dataNodeTmpFileDirPrefix;
	private int chunkBlockPeriod = 3;
	private NameNodeRemoteInterface nameNodeS;
	
	public DataNode(String nameNodeIp, int nameNodePort, int dataNodePort) {
		/* Name Node's RMI registry's address */
		this.nameNodeIp = nameNodeIp;
		this.nameNodePort = nameNodePort;
		this.dataNodePort = dataNodePort;
	}
	
	/**
	 * Initialize this Data Node by binding the remote object
	 * @throws RemoteException 
	 * @throws NotBoundException 
	 * @throws UnknownHostException 
	 */
	public void init() throws RemoteException, NotBoundException, UnknownHostException {
		
		Registry localRegistry = LocateRegistry.createRegistry(this.dataNodePort);
		DataNodeRemoteInterface dataNodeStub = (DataNodeRemoteInterface) UnicastRemoteObject.exportObject(this, 0);
		localRegistry.rebind("DataNode", dataNodeStub);
		
		
		/* join hdfs cluster */
		Registry registryOnNameNode = LocateRegistry.getRegistry(nameNodeIp, nameNodePort);
		NameNodeRemoteInterface nameNodeS = (NameNodeRemoteInterface) registryOnNameNode.lookup("NameNode");
		this.nameNodeS = nameNodeS;
		
		this.dataNodeName = InetAddress.getLocalHost().getHostAddress() + ":" + this.dataNodePort;
		
		this.dataNodeTmpFileDirPrefix = "./" + Hdfs.Core.TEMP_FILE_DIR;
		
		File nodeTmpFileFolder = new File(this.dataNodeTmpFileDirPrefix);
		if (!nodeTmpFileFolder.exists()) {
			nodeTmpFileFolder.mkdir();
		} else {
			nodeTmpFileFolder.delete();
			nodeTmpFileFolder.mkdir();
			for (File staleFile : nodeTmpFileFolder.listFiles()) {
				staleFile.delete();
			}
		}
		
		this.dataNodeTmpFileDirPrefix +=  "/" + dataNodeName;
		File dataNodeTmpFileFolder = new File(this.dataNodeTmpFileDirPrefix);
		if (!dataNodeTmpFileFolder.exists()) {
			dataNodeTmpFileFolder.mkdir();
		}
		
		System.out.println("PREFIX: " + this.dataNodeTmpFileDirPrefix);
		
		
		List<String> chunkList = formChunkReport(true);
		
		if (Hdfs.Core.DEBUG) {
			System.out.println("DEBUG DataNode.run(): " + dataNodeName + " is reporting chunks " + chunkList.toString());
		}
		
		this.dataNodeName = this.nameNodeS.join(InetAddress.getLocalHost().getHostAddress(), this.dataNodePort, chunkList);
	}

	@Override
	public void run() {
		
		try {	
			int counter = 1;
			
			List<String> chunkList = null;
			
			while (true) {
				if (counter % this.chunkBlockPeriod ==  0) {
					
					chunkList = formChunkReport(false);
					if (Hdfs.Core.DEBUG) {
						System.out.println("DEBUG DataNode.run(): " + dataNodeName + " is reporting chunks " + chunkList.toString());
					}
					this.nameNodeS.chunkReport(dataNodeName, chunkList);
				} else {
					this.nameNodeS.heartBeat(dataNodeName);
				}
				counter++;
				Thread.sleep(10000);
			}
		} catch (Exception e) {
			if (Hdfs.Core.DEBUG) {e.printStackTrace();}
		}
	}

	@Override
	public void writeToLocal(byte[] b, String chunkName, int offset) throws RemoteException {
		File chunkFile = new File(tmpFileWrapper(chunkNameWrapper(chunkName)));
		if (!chunkFile.exists()) {
			try {
				chunkFile.createNewFile();

			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		RandomAccessFile out = null;
		try {
			out = new RandomAccessFile(chunkFile, "rws");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return;
		}
		try {
			out.seek(offset);
			out.write(b);
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return;
	}
	
	public String readChunk(String chunkName) throws IOException {
		FileReader in = null;
		StringBuilder sb = new StringBuilder();
		try {
			in = new FileReader(chunkNameWrapper(chunkName));
			char[] buf = new char[4096];
			int k = 0;

			while ((k = in.read(buf)) != -1) {
				sb.append(buf, 0, k);
			}
			in.close();
		} catch (FileNotFoundException e) {
			throw new FileNotFoundException("ChunkNotFound");
		} catch (IOException e) {
			throw new IOException("IOException", e);
		}
		
		return sb.toString();
	}
	
	public String readLines(String chunkName, long pos, int numLine) throws IOException {
		RandomAccessFile file = new RandomAccessFile(chunkNameWrapper(chunkName), "r");
		long fileSize = file.length();
		file.seek(pos);
		long b = 0;
		
		StringBuilder sb = new StringBuilder();
		while (pos + b < fileSize && numLine != 0) {
			
			String line = file.readLine();
			
			sb.append(line);
			
			sb.append("\n");
			
			b += line.getBytes().length + 1;
			
			numLine--;
		}
		
		if (pos + b > fileSize) {
			sb.deleteCharAt(sb.length() - 1);
		}
		
		file.close();
		
		if (sb.length() != 0) {
			return sb.toString();
		} else {
			return null;
		}
		
	}
	
	/**
	 * Read the chunk data stores on this data node and return a buffer, the
	 * size of the buffer is set in global.Hdfs.client
	 * 
	 * @return a byte array with the chunk data, 
	 * 		   at most Hdfs.client.readBufSize
	 */
	public byte[] read(String chunkName, int offSet) throws RemoteException{
		File chunkFile = new File(chunkNameWrapper(chunkName));
		long chunkSize = chunkFile.length();
		
		if (offSet >= chunkSize) {
			return new byte[0];
		}
		RandomAccessFile in = null;
		byte[] readBuf = null;
		try {
			in = new RandomAccessFile(chunkFile, "r");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		if (offSet + Hdfs.Core.READ_BUFF_SIZE <= chunkSize) {
			readBuf = new byte[Hdfs.Core.READ_BUFF_SIZE];
		} else {
			/* offSet + readBufSize > chunkSize */
			readBuf = new byte[(int) (chunkSize - offSet)];
		}
		
		try {
			in.seek(offSet);
		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			in.read(readBuf);
		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			in.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		System.out.println("DEBUG DataNode.read(): chunkName = " + chunkName + " offSet = " + offSet + " return buffer size = " + readBuf.length);
		return readBuf;
	}
	
	@Override
	public void commitChunk(String globalChunkName, boolean valid, boolean forSysCheck) throws RemoteException {
		
		if (forSysCheck) {
			File chunkFile = new File(tmpFileWrapper(chunkNameWrapper(globalChunkName)));
			if (!chunkFile.exists()) {
				if (Hdfs.Core.DEBUG) {
					System.err.println("chunk doesn't exists");
				}
				return;
			}
			if (Hdfs.Core.DEBUG) {
				System.out.format("DEBUG DataNode.commitChunk(): Called by NameNode.SysCheck. Commit chunk (%s)\n",chunkNameWrapper(globalChunkName));
			}
			chunkFile.setWritable(false, false);
			chunkFile.renameTo(new File(chunkNameWrapper(globalChunkName)));
		} else {
			if (valid) {
				File chunkFile = new File(backupFileWrapper(chunkNameWrapper(globalChunkName)));
				if (!chunkFile.exists()) {
					System.err.println("chunk doesn't exists");
				}
				if (Hdfs.Core.DEBUG) {
					System.out.format("DEBUG DataNode.commitChunk(): Called by NameNode.CommitFile(). Commit chunk (%s)\n",backupFileWrapper(chunkNameWrapper(globalChunkName)));
				}
				chunkFile.setWritable(false, false);
				chunkFile.renameTo(new File(chunkNameWrapper(globalChunkName)));
			} else {
				if (Hdfs.Core.DEBUG) {
					System.out.format("DEBUG DataNode.commitChunk(): Called by NameNode.CommitFile(). Delete chunk (%s)\n",tmpFileWrapper(chunkNameWrapper(globalChunkName)));
				}
				File chunkFile = new File(tmpFileWrapper(chunkNameWrapper(globalChunkName)));
				chunkFile.delete();
			}
		}
		return;
	}
	
	@Override
	public void deleteChunk(String globalChunkName) throws RemoteException, IOException {
		try {
			File chunkFile = new File(chunkNameWrapper(globalChunkName));
			if (!chunkFile.exists()) {
				chunkFile = new File(tmpFileWrapper(chunkNameWrapper(globalChunkName)));
				chunkFile.delete();
				chunkFile = new File(backupFileWrapper(chunkNameWrapper(globalChunkName)));
				chunkFile.delete();
			}
			if (Hdfs.Core.DEBUG) {
				System.out.println("Delete file " + chunkFile);
			}
			chunkFile.delete();
		} catch (SecurityException e) {
			throw new IOException("Cannot delete the chunk");
		}
		
	}
	
	private String chunkNameWrapper(String globalChunkName) {
		return this.dataNodeTmpFileDirPrefix + "/" + globalChunkName + "-node-" + this.dataNodeName;
	}
	
	private String tmpFileWrapper(String name) {
		return name + ".tmp";
	}
	
	private String chunkNameUnwrapper(String localChunkName, boolean reportTmp) {
		String[] segs = localChunkName.split("-");
		if (reportTmp) {
			if (segs.length == 3 && (segs[2].equals(this.dataNodeName) || segs[2].equals(this.dataNodeName + ".tmp") || segs[2].equals(this.dataNodeName + ".backup"))) {
				return segs[0];
			}
		} else {
			if (segs.length == 3 && (segs[2].equals(this.dataNodeName))) {
				return segs[0];
			}
		}
		return null;
	}
	
	public List<String> formChunkReport(boolean withTmp) {
		List<String> chunkList = new ArrayList<String>();
		File dfDir = new File(this.dataNodeTmpFileDirPrefix);
		String[] files = dfDir.list();
	
		for (String localFileName : files) {
			String chunkName = chunkNameUnwrapper(localFileName, withTmp);
			if (chunkName != null) {
				chunkList.add(chunkName);
			}
		}
		return chunkList;
	}

	@Override
	public HDFSLineFeedCheck readLine(String chunkName) throws RemoteException,
			IOException {
		
		RandomAccessFile in
			= new RandomAccessFile(tmpFileWrapper(chunkNameWrapper(chunkName)), "rw");

		String line = in.readLine();
		
		boolean metFirstLineFeed = (in.length() != line.getBytes().length);
		
		in.close();
		
		return new HDFSLineFeedCheck(line, metFirstLineFeed);
	}

	@Override
	public void deleteFirstLine(String chunkName, boolean firstFile) throws RemoteException,
			IOException {
		
		if (firstFile) {
			File chunkFile = new File(tmpFileWrapper(chunkNameWrapper(chunkName)));
			chunkFile.renameTo(new File(backupFileWrapper(chunkNameWrapper(chunkName))));
		} else {
		
			RandomAccessFile in = 
					new RandomAccessFile(tmpFileWrapper(chunkNameWrapper(chunkName)), "rw");
			
			RandomAccessFile backup_out = 
					new RandomAccessFile(backupFileWrapper(chunkNameWrapper(chunkName)), "rw");
			
			
			String line = in.readLine();
			in.seek((line).getBytes().length);
			
			
			byte[] buff = new byte[1024];
	
			int c = 0;
			
			while ((c = in.read(buff)) != -1) {
				backup_out.write(buff, 0, c);
			}
			
			in.close();
			backup_out.close();
			
			File tmpFile = new File(tmpFileWrapper(chunkNameWrapper(chunkName)));
			tmpFile.delete();
			
		}
		
	}
	
	private String backupFileWrapper(String fileName) {
		return fileName + ".backup";
	}
	
}
