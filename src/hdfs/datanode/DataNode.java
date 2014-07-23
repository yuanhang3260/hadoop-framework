package hdfs.datanode;

import global.Hdfs;
import hdfs.io.HDFSLineFeedCheck;
import hdfs.message.CommitBackupTask;
import hdfs.message.CopyChunkTask;
import hdfs.message.DeleteChunkTask;
import hdfs.message.DeleteOrphanTask;
import hdfs.message.DeleteTempTask;
import hdfs.message.Message;
import hdfs.message.Task;
import hdfs.namenode.NameNodeRemoteInterface;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;

public class DataNode implements DataNodeRemoteInterface, Runnable {

	// TODO:Use XML to configure
	private String nameNodeIp;
	private int nameNodePort;
	
	private String dataNodeName;
	private int dataNodeRegistryPort;
	private int dataNodeServerPort;
	
	private String dirPrefix = Hdfs.Core.HDFS_TMEP;
	private String dataNodeTmpFileDirPrefix;
	
	private int chunkBlockPeriod = 3;
	private NameNodeRemoteInterface nameNodeS = null;
	
	

	public DataNode(String nameNodeIp, int nameNodePort, int dataNodeRegistryPort, int dataNodeServerPort) {
		/* Name Node's RMI registry's address */
		this.nameNodeIp = nameNodeIp;
		this.nameNodePort = nameNodePort;
		this.dataNodeRegistryPort = dataNodeRegistryPort;
		this.dataNodeServerPort = dataNodeServerPort;
		System.out.println("REIGSITRY PORT: " + dataNodeRegistryPort);
		System.out.println("SERVER PORT:" + dataNodeServerPort);
	}

	/**
	 * Initialize this Data Node by binding the remote object
	 * 
	 * @throws NotBoundException
	 * @throws IOException 
	 */
	public void init() throws NotBoundException,
			IOException {

		/*-------------- Export and bind NameNode --------------*/
		Registry localRegistry = LocateRegistry
				.createRegistry(this.dataNodeRegistryPort);
		
		DataNodeRemoteInterface dataNodeStub = 
				(DataNodeRemoteInterface) UnicastRemoteObject.exportObject(this, 0);
		
		localRegistry.rebind(Hdfs.Core.DATA_NODE_SERVICE_NAME, dataNodeStub);

		/*-------------- Get NameNode Stub --------------*/
		Registry nameNodeR = LocateRegistry.getRegistry(nameNodeIp,
				nameNodePort);
		this.nameNodeS = (NameNodeRemoteInterface) nameNodeR
				.lookup(Hdfs.Core.NAME_NODE_SERVICE_NAME);

		this.dataNodeName = InetAddress.getLocalHost().getHostAddress() + ":"
				+ this.dataNodeRegistryPort;

		/*-------------- Initiate Temp Directory --------------*/
		File nodeTmpFile = new File(this.dirPrefix);
		if (!nodeTmpFile.exists()) {
			nodeTmpFile.mkdir();
		}

		File dataNodeTmpFile = new File(this.dirPrefix + "/"
				+ this.dataNodeName);
		if (!dataNodeTmpFile.exists()) {
			dataNodeTmpFile.mkdir();
		} else {
			dataNodeTmpFile.delete();
			dataNodeTmpFile.mkdir();
		}

		this.dirPrefix += "/" + this.dataNodeName;

		this.dataNodeTmpFileDirPrefix = this.dirPrefix;
		
		
		/*----------- Initiate DataNode Server port ------------*/
		Server server = new Server(this.dataNodeServerPort);
		Thread serverTh = new Thread(server);
		serverTh.start();
		
		
		/*----------- Report Current Files in Folder -----------*/
		List<String> chunkList = formChunkReport(true);
		if (Hdfs.Core.DEBUG) {
			System.out.println("DEBUG DataNode.run(): " + dataNodeName
					+ " is reporting chunks " + chunkList.toString());
		}

		/*-------------- Join the cluster --------------*/
		Message message = this.nameNodeS.join(InetAddress.getLocalHost()
				.getHostAddress(), this.dataNodeRegistryPort, this.dataNodeServerPort, chunkList);
		
		for (Task task : message.getTaskList()) {
			
			if (task instanceof DeleteOrphanTask) {

				deleteOrphanTaskHandler((DeleteOrphanTask) task);
				
			} 
		}
	}

	@Override
	/**
	 * This is the routine thread for DataNode periodically pings NameNode
	 */
	public void run() {
		
		int counter = 1;

		while (true) {
					
			/*------------ DataNode sends chunk report every 3 heart beat -----------*/
			
			Message message = sendHeartBeat(counter++ % this.chunkBlockPeriod == 0);
			
			for (Task task : message.getTaskList()) {
				taskHandler(task);
			}

			try {
				Thread.sleep(Hdfs.Core.HEART_BEAT_FREQ);
			} catch (InterruptedException e) {
				if (Hdfs.Core.DEBUG) { e.printStackTrace(); }
			}
			
		}

	}
	
	
	private Message sendHeartBeat(boolean reportChunk) {
		
		Message message = null;
		
		try {
			if (reportChunk) {
	
				List<String> chunkList = formChunkReport(false);
				if (Hdfs.Core.DEBUG) {
					System.out.print("DataNode.sendHeartBeat(): Report chunk: ");
					for (String chunkName : chunkList) {
						System.out.print("\t" + chunkName);
					}
					System.out.println();
				}
				message = this.nameNodeS.chunkReport(dataNodeName, chunkList);
				
			} else {
				
				message = this.nameNodeS.heartBeat(dataNodeName);
				
			}
			
		} catch (Exception e) {
			
			if (Hdfs.Core.DEBUG) { e.printStackTrace(); }
			
		}
		
		return message;
	}

	@Override
	public void writeToLocal(byte[] b, String chunkName, int offset) throws IOException {
		
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
			if (Hdfs.Core.DEBUG) { e.printStackTrace();}
			throw new IOException("Cannot open the file.", e);
		}
		
		try {
			out.seek(offset);
			out.write(b);
			out.close();
		} catch (IOException e) {
			if (Hdfs.Core.DEBUG) { e.printStackTrace(); }
			throw new IOException("Failed to write.", e);
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

	public String readLines(String chunkName, long pos, int numLine)
			throws IOException {
		RandomAccessFile file = new RandomAccessFile(
				chunkNameWrapper(chunkName), "r");
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
	 * @return a byte array with the chunk data, at most Hdfs.client.readBufSize
	 */
	public byte[] read(String chunkName, int offSet) throws IOException {
		File chunkFile = new File(chunkNameWrapper(chunkName));
		long chunkSize = chunkFile.length();

		if (offSet >= chunkSize) {
			return new byte[0];
		}
		RandomAccessFile in = null;
		byte[] readBuf = null;
		
		for (int i = 0; i < Hdfs.Core.INCONSISTENCY_LATENCY / Hdfs.Core.HEART_BEAT_FREQ; i++) {
			try {
				in = new RandomAccessFile(chunkFile, "r");
				break;
			} catch (FileNotFoundException e) {
				if (Hdfs.Core.DEBUG) { 
					e.printStackTrace(); 
					System.out.println("DEBUG Datanode.read(): Failed to open the file. Wait for a heart beat");
					}
			}
			try {
				Thread.sleep(Hdfs.Core.HEART_BEAT_FREQ);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		if (in == null) {
			throw new FileNotFoundException(String.format("Chunk cannot be open: chunk name:%s", chunkFile.getName()));
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

		System.out.println("DEBUG DataNode.read(): chunkName = " + chunkName
				+ " offSet = " + offSet + " return buffer size = "
				+ readBuf.length);
		return readBuf;
	}

	private String chunkNameWrapper(String globalChunkName) {
		return this.dataNodeTmpFileDirPrefix + "/" + globalChunkName + "-node-"
				+ this.dataNodeName;
	}

	private String tmpFileWrapper(String name) {
		return name + ".tmp";
	}

	private String chunkNameUnwrapper(String localChunkName, boolean reportTmp) {
		String[] segs = localChunkName.split("-");
		if (reportTmp) {
			if (segs.length == 3
					&& (segs[2].equals(this.dataNodeName)
							|| segs[2].equals(this.dataNodeName + ".tmp") || segs[2]
								.equals(this.dataNodeName + ".backup"))) {
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

		RandomAccessFile in = new RandomAccessFile(
				tmpFileWrapper(chunkNameWrapper(chunkName)), "rw");

		String line = in.readLine();

		boolean metFirstLineFeed = (in.length() != line.getBytes().length);

		in.close();

		return new HDFSLineFeedCheck(line, metFirstLineFeed);
	}

	@Override
	public void deleteFirstLine(String chunkName, boolean firstFile)
			throws RemoteException, IOException {

		if (firstFile) {
			File chunkFile = new File(
					tmpFileWrapper(chunkNameWrapper(chunkName)));
			chunkFile.renameTo(new File(
					backupFileWrapper(chunkNameWrapper(chunkName))));
		} else {

			RandomAccessFile in = new RandomAccessFile(
					tmpFileWrapper(chunkNameWrapper(chunkName)), "rw");

			RandomAccessFile backup_out = new RandomAccessFile(
					backupFileWrapper(chunkNameWrapper(chunkName)), "rw");

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
	
	
	private void taskHandler(Task task) {
		
		if (task instanceof CommitBackupTask) {
			commitBackupTaskHandler((CommitBackupTask)task);
		} 
		
		else if (task instanceof CopyChunkTask) {
			copyChunkTaskHandler((CopyChunkTask)task);
		}
		
		else if (task instanceof DeleteChunkTask) {
			deleteChunkTaskHandler((DeleteChunkTask)task);
		}
		
		else if (task instanceof DeleteOrphanTask) {
			deleteOrphanTaskHandler((DeleteOrphanTask)task);
		}  
		
		else if (task instanceof DeleteTempTask) {
			deleteTempTaskHandler((DeleteTempTask)task);
		} 
		
	}
	private void deleteOrphanTaskHandler (DeleteOrphanTask task) {
		
		String orphanChunkName = task.getChunkName();
		
		File orphanChunk = new File(chunkNameWrapper(orphanChunkName));
		orphanChunk.delete();
		
		orphanChunk = new File(tmpFileWrapper(chunkNameWrapper(orphanChunkName)));
		orphanChunk.delete();
		
		orphanChunk = new File(backupFileWrapper(chunkNameWrapper(orphanChunkName)));
		orphanChunk.delete();
	}
	
	private void commitBackupTaskHandler (CommitBackupTask task) {
		
		String committedChunkName = chunkNameWrapper((task.getChunkName()));
		File committedChunk = new File(committedChunkName);
		
		String backupChunkName = backupFileWrapper(chunkNameWrapper(task.getChunkName()));
		File backupChunk = new File(backupChunkName);
		
		backupChunk.renameTo(committedChunk);
		backupChunk.setWritable(false, false);
	}
	
	private void deleteTempTaskHandler(DeleteTempTask task) {
		
		String tmpFileName = chunkNameWrapper(task.getChunkName());
		File tmpFile = new File(tmpFileName);
		tmpFile.delete();

	}
	
	private void copyChunkTaskHandler(CopyChunkTask task) {
		
		String dataNodeIp = null;
		try {
			dataNodeIp = Inet4Address.getLocalHost().getHostAddress();
		} catch (UnknownHostException e) {
			if (Hdfs.Core.DEBUG) {e.printStackTrace();}
			return;
		}
		
		if (dataNodeIp.equals(task.getSrcDataNodeIp()) && this.dataNodeServerPort == task.getSrcDataNodeServerPort()) {
			
			if (Hdfs.Core.DEBUG) {
				System.out.println("DataNoe.copyChunkTaskHandler(): NameNode requires DataNode to copy chunk from itself. ");
			}
			
			return;
		}
		
		if (Hdfs.Core.DEBUG) {
			System.out.println("DataNoe.copyChunkTaskHandler(): Start copy chunk task.");
		}
		
		
		String chunkFileName = chunkNameWrapper(task.getChunkName());
		File chunkFile = new File(chunkFileName);
		
		if (chunkFile.exists()) {
			return;
		}
		
		try {
			Socket soc = new Socket(task.getSrcDataNodeIp(), task.getSrcDataNodeServerPort());
			
			PrintWriter out = new PrintWriter(new OutputStreamWriter(soc.getOutputStream()));
			BufferedInputStream in = new BufferedInputStream(soc.getInputStream());
			BufferedOutputStream fin =
					new BufferedOutputStream(new FileOutputStream(chunkFile));
			
			out.println(task.getChunkName());
			out.flush();
			
			byte[] buff = new byte[Hdfs.Core.WRITE_BUFF_SIZE];
			int len = 0;
			
			while ((len = in.read(buff)) != -1) {
				fin.write(buff, 0, len);
			}
			
			out.close();
			in.close();
			fin.close();
			
		} catch (UnknownHostException e) {
			if (Hdfs.Core.DEBUG) { e.printStackTrace(); }
		} catch (IOException e) {
			if (Hdfs.Core.DEBUG) { e.printStackTrace(); }
		}
		
		if (Hdfs.Core.DEBUG) {
			System.out.println("DataNoe.copyChunkTaskHandler(): Finish Copy");
		}
		chunkFile.setWritable(false, false);
	}
	
	private void deleteChunkTaskHandler(DeleteChunkTask task) {
		
		String deleteChunkName = chunkNameWrapper(task.getChunkName());
		File deleteChunk = new File(deleteChunkName);
		deleteChunk.delete();
		
	}
	
	private class Server implements Runnable {
		
		int serverPort;
		ServerSocket serverSoc;
		
		public Server(int port) throws IOException {
			
			this.serverPort = port;
			System.out.println("Server port:" + this.serverPort);
			this.serverSoc = new ServerSocket(this.serverPort);
			
		}
		
		@Override
		public void run() {
			
			while (true) {
				try {
					respond();
				} catch (IOException e) {
					if (Hdfs.Core.DEBUG) {e.printStackTrace();}
				}
			}
			
		}
		
		private void respond() throws IOException {
			Socket soc = this.serverSoc.accept();
			
			/* Set up */
			BufferedReader in = new BufferedReader(
					new InputStreamReader(soc.getInputStream()));
			
			BufferedOutputStream out = new BufferedOutputStream(soc.getOutputStream());
			
			String request = in.readLine();
			
			File chunkFile = new File(chunkNameWrapper(request));
			BufferedInputStream fin = new BufferedInputStream(new FileInputStream(chunkFile));
			
			byte[] buff = new byte[Hdfs.Core.READ_BUFF_SIZE];
			int len = 0;
			
			while ((len = fin.read(buff)) != -1) {
				
				out.write(buff, 0, len);
				
			}
			
			in.close();
			out.close();
			fin.close();
		}
		
	}
}
