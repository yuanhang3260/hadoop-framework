package hdfs.namenode;

import global.Hdfs;
import hdfs.datanode.DataNodeRemoteInterface;
import hdfs.io.DataNodeEntry;
import hdfs.io.HDFSChunk;
import hdfs.io.HDFSFile;
import hdfs.message.CommitBackupTask;
import hdfs.message.CopyChunkTask;
import hdfs.message.DeleteChunkTask;
import hdfs.message.DeleteOrphanTask;
import hdfs.message.DeleteTempTask;
import hdfs.message.Message;
import hdfs.message.Task;

import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class NameNode implements NameNodeRemoteInterface{
	volatile long chunkNaming;
	private Map<String, DataNodeAbstract> dataNodeTbl;
	private Map<String, HDFSFile> fileTbl;

	private NameNodeRemoteInterface nameNodeStub;
	private int port;
	
	private Map<String, List<Task>> dataNodeTaskTbl;
	private long namer;
//	private Object sysCheckSync = new Object();
	
	/*-------------------------- Constructor ---------------------*/
	/**
	 * @param The registry port of NameNode for RMI
	 */
	public NameNode(int port) {
		this.dataNodeTbl = new ConcurrentHashMap<String, DataNodeAbstract>();
		this.fileTbl = new ConcurrentHashMap<String, HDFSFile>();

		this.dataNodeTaskTbl = new ConcurrentHashMap<String, List<Task>>();
		this.chunkNaming = 1;
		this.port = port;
	}
	
	
	/**
	 * Initialize the NameNode to start the service.
	 * @throws RemoteException Default remote exception.
	 */
	public void init() throws RemoteException {
		
		/* Export and bind RMI */
		Registry registry = null;

		registry = LocateRegistry.createRegistry(this.port);
		this.nameNodeStub = (NameNodeRemoteInterface) UnicastRemoteObject.exportObject(this, 0);
		registry.rebind("NameNode", nameNodeStub);
		
		Thread systemCheckThread = new Thread(new SystemCheck());
		systemCheckThread.start();
	}
	
	
	/*------------------------- RMI by DataNode----------------------------*/
	
	@Override
	public synchronized Message heartBeat(String dataNodeName) {
		
		this.dataNodeTbl.get(dataNodeName).timestamp();
		
		Message message = messageConstructor(dataNodeName);
		
		if (Hdfs.Core.DEBUG) {
			System.out.format("DEBUG NameNode.heartBeat(): Remained task on <%s> list:\t", dataNodeName);
			
			for (Task task: this.dataNodeTaskTbl.get(dataNodeName)) {
				
				System.out.format("%s<%s>\t", task.getClass().getName(), task.getTid());
				
			}
			
			System.out.println();
			
		}
		
		return message;
	}
	
	@Override
	public Message chunkReport(String dataNodeName, List<String> chunkList) throws RemoteException {
		
		this.dataNodeTbl.get(dataNodeName).timestamp();
		
		DataNodeAbstract dataNodeInfo = this.dataNodeTbl.get(dataNodeName);
		dataNodeInfo.chunkList = chunkList;
		
		Message message = messageConstructor(dataNodeName);
		return message;
	}
	
	@Override
	public Message join(String ip, int registryPort, int serverPort, List<String> chunkNameList) throws RemoteException {
		
		String dataNodeName = ip + ":" + registryPort;  //TODO: change the naming method
		
		/*--------- Set DataNode Stub to DataNodeInfo ---------*/
		DataNodeAbstract dataNodeInfo = new DataNodeAbstract(ip, registryPort, serverPort, dataNodeName);
		
		/*--------- Put DataNode information to NameNode ---------*/
		dataNodeTbl.put(dataNodeName, dataNodeInfo);
		
		/*--------- Create DataNode task queue -------------------*/
		dataNodeTaskTbl.put(dataNodeName, Collections.synchronizedList(new ArrayList<Task>()));
		
		
		/*--------- Check if these stale chunks are valuable --------*/
		List<String> dataNodeChunkList = orphanCheck(dataNodeName, chunkNameList); 
	
		Message message = new Message();
		
		List<Task> taskList = this.dataNodeTaskTbl.get(dataNodeName);
		List<Task> deletedTask = new ArrayList<Task>();
		synchronized (taskList) {
			for (Task task : taskList) {
				if (task instanceof DeleteChunkTask) {
					message.addTask(task);
					deletedTask.add(task);
				}
			}
			
			for (Task task : deletedTask) {
				taskList.remove(task);
			}
		}
		
		dataNodeInfo.chunkList = dataNodeChunkList;
		return message;
	}
	
	
	@Override
	public synchronized HDFSFile create(String filePath) throws RemoteException, IOException {
		if (this.fileTbl.containsKey(filePath)) {
			if (Hdfs.Core.DEBUG) {
				System.out.format("File(%s) is duplicated", filePath);
			}
			throw new IOException(String.format("File(%s) is duplicated", filePath));
		}
		HDFSFile newFile = new HDFSFile(filePath, Hdfs.Core.REPLICA_FACTOR, this.nameNodeStub);
		
		this.fileTbl.put(filePath, newFile);
		
		return newFile;
	}
	
	@Override
	public HDFSFile open(String fileName) throws RemoteException {
		HDFSFile file = this.fileTbl.get(fileName);
		return file;
	}
	
	
	@Override
	public String listFileTbl() throws RemoteException {
		
		StringBuilder sb = new StringBuilder();
		
		Set<String> filePaths = this.fileTbl.keySet();
		
		int i = 0;
		sb.append("------------------------------>");
		for (String fileName : filePaths) {
			
			sb.append(String.format("\n\n[%d]\t%s\t",i,fileName));
			
			HDFSFile file = this.fileTbl.get(fileName);
			List<HDFSChunk> chunkList = file.getChunkList();
			
			for (int j = 0; j < chunkList.size(); j++) {
				
				HDFSChunk chunk = chunkList.get(j);
				
				sb.append(String.format("\n\t[%d-%d]<Chunk:%s>:",i, j, chunk.getChunkName()));
				
				List<DataNodeEntry> entries = chunk.getAllLocations();
				for (int m = 0; m < entries.size(); m++) {
					
					sb.append(String.format("%s\t",entries.get(m).getNodeName()));
					
				}
			}	
			i++;
		}
		sb.append("\n<------------------------------\n");
		return sb.toString();
	}
		
	@Override
	public void delete(String path) throws IOException {
		
		HDFSFile file = this.fileTbl.get(path);
		if (file == null) {
			throw new IOException("No such file.");
		}
		
		for (HDFSChunk chunk : file.getChunkList()) {
			
			for (DataNodeEntry dataNode : chunk.getAllLocations()) {
				
				String tid = (new Date()).getTime() + "";
				DeleteChunkTask task = new DeleteChunkTask(tid, chunk.getChunkName(), file.getName());
				
				String dataNodeName = dataNode.getNodeName();
				List<Task> taskList = this.dataNodeTaskTbl.get(dataNodeName);
				taskList.add(task);
			}
		}
		
		this.fileTbl.remove(path);
	}
	
	@Override
	public PriorityQueue<String> listFiles() throws RemoteException {
		
		PriorityQueue<String> fileList = new PriorityQueue<String>();
		Set<String> fileNameSet = this.fileTbl.keySet();
		for (String fileName : fileNameSet) {
			fileList.offer(fileName);
		}
		
		return fileList;
	}
	
	@Override
	public synchronized String nameChunk() {
		String chunkName = String.format("%d", new Date().getTime());
		return chunkName;
	}
	
	public synchronized List<DataNodeEntry> select(int replicaFactor) {
		
		Collection<DataNodeAbstract> dataNodeCollcetion = this.dataNodeTbl.values();
		DataNodeAbstract[] dataNodeArray = dataNodeCollcetion.toArray(new DataNodeAbstract[0]);
		Arrays.sort(dataNodeArray);
		
		List<DataNodeEntry> entries = new ArrayList<DataNodeEntry>();
		
		for (int i = 0; i < dataNodeArray.length && entries.size() < replicaFactor; i++) {
			
			if (dataNodeArray[i].isAvailable()) {
				
				entries.add(new DataNodeEntry(dataNodeArray[i].getDataNodeRegistryIp(), 
						dataNodeArray[i].getDataNodeRegistryPort(), 
						dataNodeArray[i].getDataNodeName()));
			} else {
				
				if (Hdfs.Core.DEBUG) {
					System.out.format("DEBUG NameNode.select(): Try to select DataNode<%s> but it is NOT Available.\n", dataNodeArray[i].getDataNodeName());
				}
				
			}
			
		}
		
		if (Hdfs.Core.DEBUG) {
			
			System.out.print("DEBUG NameNode.select(): Selected the following DataNodeEntries:");
			for (int i = 0; i < entries.size(); i++) {
				System.out.format("%s", entries.get(i).getNodeName());
			}
			System.out.println();
		}
		
		return entries;
	}


	public synchronized void commitFile(HDFSFile file) throws RemoteException {
		
		List<HDFSChunk> totalChunkList = file.getTotalChunkList();
		
		List<HDFSChunk> validChunkList = file.getChunkList();
		
		for (HDFSChunk chunk : totalChunkList) {
			
			for (DataNodeEntry dataNodeEntry : chunk.getAllLocations()) {
				
				String dataNodeName = dataNodeEntry.getNodeName();
				List<Task> taskList = this.dataNodeTaskTbl.get(dataNodeName);
				if (validChunkList.contains(chunk)) {
					String tid = (new Date()).getTime() + "";
					taskList.add(new CommitBackupTask(tid, chunk.getChunkName()));
				} else {
					String tid = (new Date()).getTime() + "";
					taskList.add(new DeleteTempTask(tid, chunk.getChunkName()));
				}	
			}
		}
		
		file.setCommitTime(new Date());
		this.fileTbl.put(file.getName(), file);
	}
	
	/**
	 * This method checks the whether orphans exists in DataNode's chunk name list.
	 * 
	 * @param dataNodeName
	 * @param chunkNameList_DataNode
	 * @return
	 */
	private List<String> orphanCheck(String dataNodeName, List<String> chunkNameList_DataNode) {
		
		Set<String> fileNameSet = this.fileTbl.keySet();
		
		Set<String> chunkSet_NameNode = new HashSet<String>();
		
		for (String fileName : fileNameSet) {
			
			HDFSFile file = this.fileTbl.get(fileName);  //Check if the file is new created
			
			synchronized(file) {
				
				Iterator<HDFSChunk> chunkIt = file.getChunkList().iterator();
				
				while (chunkIt.hasNext()) {
					
					HDFSChunk chunk = chunkIt.next();
					chunkSet_NameNode.add(chunk.getChunkName());
					
				}
				
			}
		}
		
		List<String> deletedChunkName = new ArrayList<String>();
		for (String chunkName : chunkNameList_DataNode) {
			
			/* If the NameNode doesn't have such chunk */
			if (!chunkSet_NameNode.contains(chunkName)) {
				
				/* Add DeleteChunkTask to the list */
				String tid = String.format("%d", (new Date()).getTime());
				this.dataNodeTaskTbl.get(dataNodeName).add(new DeleteOrphanTask(tid, chunkName));
				deletedChunkName.add(chunkName);
			}			
		}
		
		for (String chunkName : deletedChunkName) {
			chunkNameList_DataNode.remove(chunkName);
		}
		
		return chunkNameList_DataNode;
	}
	
	
	private Message messageConstructor(String dataNodeName) {
		
		/*--------- Prepare return message --------*/
		Message message = new Message();
		
		
		/*--------- Find corresponding task list --------*/
		List<Task> taskList = this.dataNodeTaskTbl.get(dataNodeName);
		
		
		/*--------- Add task to message --------*/
		List<Task> deletedTask = new ArrayList<Task>();
		
		synchronized (taskList) {
			for (Task task : taskList) {
				message.addTask(task);
				deletedTask.add(task);
				task.sentTask();
				
			}
			
			for (Task task : deletedTask) {
				taskList.remove(task);
			}
		}
		
		for (Task task : deletedTask) {
			taskHandler(task, dataNodeName);
		}
		
		return message;
	}
	
	private void taskHandler(Task task, String dataNodeName) {
		/*
		 * [1] Add chunk location to file table because of new replica of chunk
		 * [2] Add chunk name to the DataNode's chunk list because the DataNode
		 *     will execute copy chunk.
		 *     
		 * */
		if (task instanceof CopyChunkTask) {
			
			HDFSFile file = this.fileTbl.get(((CopyChunkTask) task).getFileName());
			
			if (file == null) {
				if (Hdfs.Core.DEBUG) {
					System.out.format("NameNode.taskHandler(): Branch<CopyChunkTask> File(%s) deosn't exist.", ((CopyChunkTask) task).getFileName());
				}
			}
			
			for (HDFSChunk chunk : file.getChunkList()) {
				if (chunk.getChunkName().equals(((CopyChunkTask) task).getChunkName())) {
					DataNodeEntry newLocation = this.dataNodeTbl.get(dataNodeName).generateDataNodeEntry();
					chunk.getAllLocations().add(newLocation);
					break;
				}
			}
			
			DataNodeAbstract dataNode = this.dataNodeTbl.get(dataNodeName);
			dataNode.chunkList.add(((CopyChunkTask) task).getChunkName());
			
		} 
		
		/*
		 * [1]. Delete the chunk from file table 
		 *      because of deleting file or over-replicating chunk.
		 * [2]. Delete the chunk from the DataNode table 
		 *      because the DataNode will execute deleting task
		 * 
		 * */
		
		else if (task instanceof DeleteChunkTask) {
			
			HDFSFile file = this.fileTbl.get(((DeleteChunkTask) task).getFileName());
			if (file != null) { //DeleteChunkTask may be added by delete file so that HDFSFile entry is removed
				for (HDFSChunk chunk : file.getChunkList()) {
					
					if (chunk.getChunkName().equals(((DeleteChunkTask) task).getChunkName())) { // found the chunk info
						
						/*-------- Delete the DataNode entry from this chunk's locations -------------*/
						DataNodeEntry todeleteEntry = null;
						for (DataNodeEntry entry : chunk.getAllLocations()) {
							if (entry.getNodeName().equals(dataNodeName)) {
								todeleteEntry = entry;
							}
						}
						if (todeleteEntry != null) {
							chunk.getAllLocations().remove(todeleteEntry);
						}
					}
					
					break;
				}
			}
			
			DataNodeAbstract dataNode = this.dataNodeTbl.get(dataNodeName);
			dataNode.chunkList.remove(((DeleteChunkTask) task).getChunkName());
		} 
		
		/*
		 * Delete the orphan chunk from DataNode's chunk list 
		 * because the DataNode will execute the task
		 * 
		 * */
		else if (task instanceof DeleteOrphanTask) {
			
			DataNodeAbstract dataNode = this.dataNodeTbl.get(dataNodeName);
			dataNode.chunkList.remove(((DeleteOrphanTask) task).getChunkName());
			
		}
	}
	
	
	/* ------------------------------ Nested Class ----------------------------*/
	/**
	 * This nested class is to keep track of detailed information of each DataNode.
	 * Unlike DataNodeEntry which is used by other HDFS classes to locate and request
	 * service from DataNode, this class is used to store more status information of
	 * DataNode.
	 * 
	 * @author Jeremy Fu and Kim Wei 
	 *
	 */
	private class DataNodeAbstract implements Comparable<DataNodeAbstract> {

		//TODO:a dataNode stub variable
		private String dataNodeName;
		private String dataNodeRegistryIP;
		private int dataNodeRegistryPort;
		private int dataNodeServerPort;
		
		private DataNodeRemoteInterface dataNodeS;
		
		private boolean available;
		private List<String> chunkList;
		private long latestHeartBeat;
		
		public DataNodeAbstract(String ip, int port, int serverPort, String name) {
			this.chunkList = new ArrayList<String>();
			this.latestHeartBeat = new Date().getTime();
			this.dataNodeRegistryIP = ip;
			this.dataNodeRegistryPort = port;
			this.dataNodeServerPort = serverPort;
			this.dataNodeName = name;
			this.available = true;
		}
		
		@Override
		/**
		 * This is an overridden method which gives the way to prioritize
		 * the DataNodes whose chunks are less than others.
		 */
		public int compareTo(DataNodeAbstract compareDataNodeInfo) {
			int compareQuantity = compareDataNodeInfo.chunkList.size();
			return this.chunkList.size() - compareQuantity;
		}
		
		/**
		 * If the NameNode receives DataNode's heart beat when the network
		 * partition has gone, the DataNode is enabled again.
		 */
		private void checkAvailability() {
			if ((new Date()).getTime() - this.latestHeartBeat > Hdfs.Core.PARTITION_TOLERANCE) {
				this.available = false;
				if (Hdfs.Core.DEBUG) {
					System.out.println("DEBUG NameNode.SystemCheck.run(): Disable DataNode:" + this.dataNodeName);
				}
			}	
		}
		
		public void timestamp() {
			this.latestHeartBeat = (new Date()).getTime();
			this.available = true;
		}
		
		/**
		 * Check if the DataNode is regarded as available to service.
		 * @return The availability of DataNode.
		 */
		private boolean isAvailable() {
			return this.available;
		}
		
		private void setDataNodeStub(DataNodeRemoteInterface stub) {
			this.dataNodeS = stub;
		}
		
		
		public String getDataNodeRegistryIp() {
			return this.dataNodeRegistryIP;
		}
		
		public int getDataNodeRegistryPort() {
			
			return this.dataNodeRegistryPort;
			
		}
		
		public String getDataNodeName() {
			return this.dataNodeName;
		}
		
		public DataNodeEntry generateDataNodeEntry() {
			
			DataNodeEntry entry = 
					new DataNodeEntry(this.dataNodeRegistryIP,
							this.dataNodeRegistryPort, this.dataNodeName);
			
			return entry;
		}
		
		public int getDataNodeServerPort() {
			return this.dataNodeServerPort;
		}		
		
	}
	
	/**
	 * This runnable class acts as system check 
	 * @author JeremyFu and Kim Wei
	 *
	 */
	private class SystemCheck implements Runnable {
		
		public void run() {
			
			while(true) {
				
				check();

				try {
					Thread.sleep(Hdfs.Core.SYSTEM_CHECK_PERIOD);
				} catch (InterruptedException e) {
					if (Hdfs.Core.DEBUG) {
						e.printStackTrace();
					}
				}
			}
		}
		
		private void check() {
				
			/*--------------- Initialization -------------*/
			HashMap<String, ChunkStatisticsForDataNode> chunkAbstractFromDataNode
				= new HashMap<String, ChunkStatisticsForDataNode>();
			HashMap<String, ChunkStatisticsForNameNode> chunkAbstractFromNameNode
				= new HashMap<String, ChunkStatisticsForNameNode>();
			Set<String> toDeleteFilesName = new HashSet<String>();
			
			if (Hdfs.Core.DEBUG) {
				System.out.println("DEBUG NameNode.SystemCheck.run(): Start SystemCheck.");
			}
				
			/*------------ Check availability of DataNode --------------*/
			Date now = new Date();
			
			Set<String> dataNodeNameSet = null;
			
			dataNodeNameSet = NameNode.this.dataNodeTbl.keySet(); //TODO: block datanodetable
			
			for (String dataNode : dataNodeNameSet) {
				DataNodeAbstract dataNodeInfo = NameNode.this.dataNodeTbl.get(dataNode);
				dataNodeInfo.checkAvailability();	
			}

			
			
			
			/* Obtain chunk abstract from name node */
			
			Set<String> filePaths = NameNode.this.fileTbl.keySet();
			
			for (String filePath : filePaths) {
				
				HDFSFile file = NameNode.this.fileTbl.get(filePath);
				
				synchronized (file) {
				
					if (file.getCommitTime() != null &&
							(now.getTime() - file.getCommitTime().getTime()) > Hdfs.Core.INCONSISTENCY_LATENCY) {
						
						List<HDFSChunk> chunkList = file.getChunkList();
						for (HDFSChunk chunk : chunkList) {								
							chunkAbstractFromNameNode.put(chunk.getChunkName(),
								new ChunkStatisticsForNameNode(file.getReplicaFactor(), 
										file.getName()));
							
						}
					}
				}
			}
			
			/*
			 * Check each file's each chunk is located on those DataNodes.
			 * */
			for (String filePath : filePaths) {
				
				HDFSFile file = NameNode.this.fileTbl.get(filePath);
				
				for (HDFSChunk chunk : file.getChunkList()) {
					
					DataNodeEntry removeEntry = null;
					
					for (DataNodeEntry entry : chunk.getAllLocations()) {
						
						DataNodeAbstract dataNode = NameNode.this.dataNodeTbl.get(entry.getNodeName());
						
						if (!dataNode.chunkList.contains(chunk.getChunkName())) {
							removeEntry  = entry;
							break;
						}
						
					}
					
					chunk.removeDataNodeEntry(removeEntry);
					
				}
				
			}
				
				/* Obtain chunk abstract from data node */
			Collection<DataNodeAbstract> dataNodes = NameNode.this.dataNodeTbl.values();
			for (DataNodeAbstract dataNode : dataNodes) {
				if (!dataNode.isAvailable()) {
					continue;
				}
				
				synchronized (dataNode) {
					for (String chunkName : dataNode.chunkList) {
						if (chunkAbstractFromDataNode.containsKey(chunkName)) {
							ChunkStatisticsForDataNode chunkStat = chunkAbstractFromDataNode.get(chunkName);
							chunkStat.replicaNum++;
							chunkStat.getLocations().add(dataNode.dataNodeName);
						} else {
							ChunkStatisticsForDataNode chunkStat = new ChunkStatisticsForDataNode();
							chunkStat.replicaNum = 1;
							chunkStat.getLocations().add(dataNode.dataNodeName);
							chunkAbstractFromDataNode.put(chunkName, chunkStat);
						}
					}
				}
			}
				
			/* Delete orphan chunks */
			Set<String> chunksOnDataNode = chunkAbstractFromDataNode.keySet();
			
			if (Hdfs.Core.DEBUG) {
				System.out.println("DEBUG NameNode.SystemCheck.run(): chunks collected from data node: " + chunksOnDataNode.toString());
			}
			
			for (String chunkOnDataNode : chunksOnDataNode) {
				if (!chunkAbstractFromNameNode.containsKey(chunkOnDataNode)) {
					
					if (Hdfs.Core.DEBUG) {
						System.out.format("DEBUG NameNode.SystemCheck.run(): chunk(%s) is orphan.\n",chunkOnDataNode);
					}
					
					ChunkStatisticsForDataNode chunkStat = chunkAbstractFromDataNode.get(chunkOnDataNode);
					
					for (String dataNodeName : chunkStat.dataNodes) {
						
						List<Task> dataNodeTaskList = NameNode.this.dataNodeTaskTbl.get(dataNodeName);
						
						String tid = (new Date()) + "";
						Task task = new DeleteOrphanTask(tid, chunkOnDataNode);
						dataNodeTaskList.add(task);
					}
					
				}
			}
				
				
				/* Compare two abstracts */
			Set<String> chunksOnNameNode = chunkAbstractFromNameNode.keySet();
			for (String chunkOnNameNode : chunksOnNameNode) {
				
				ChunkStatisticsForDataNode chunkStat = null;
				if(chunkAbstractFromDataNode.containsKey(chunkOnNameNode)) {
					chunkStat = chunkAbstractFromDataNode.get(chunkOnNameNode);
				} else {
					//This chunk on NameNode cannot be found on any DataNode.
					toDeleteFilesName.add(chunkAbstractFromNameNode.get(chunkOnNameNode).filePath);
					continue;
				}
				
				int replicaFac = chunkAbstractFromNameNode.get(chunkOnNameNode).replicaFactor;
				if (chunkStat.replicaNum == replicaFac) {
					if (Hdfs.Core.DEBUG) {
						System.out.println("DEBUG NameNode.SystemCheck.run(): chunk(" + chunkOnNameNode + ") is OKAY");
					}
				} else if (chunkStat.replicaNum < replicaFac) {
					
					if (Hdfs.Core.DEBUG) {
						String debugInfo = String.format("DEBUG NameNode.SystemCheck.run(): chunk(%s) is LESS THAN RF. STAT: num=%d, rf=%d", chunkOnNameNode, chunkStat.replicaNum, replicaFac);
						System.out.println(debugInfo);
					}
					
					String srcDataNodeName = chunkStat.dataNodes.get(0);
					String srcDataNodeIp = NameNode.this.dataNodeTbl.get(srcDataNodeName).getDataNodeRegistryIp();
					int srcDatanodeServerPort = NameNode.this.dataNodeTbl.get(srcDataNodeName).getDataNodeServerPort();
					
					List<DataNodeEntry> dstDataNodes = NameNode.this.select(replicaFac - chunkStat.replicaNum);
					
					for (DataNodeEntry entry : dstDataNodes) {
						List<Task> taskList = NameNode.this.dataNodeTaskTbl.get(entry.getNodeName());
						String tid = (new Date()).getTime() + "";
						Task task = new CopyChunkTask(tid, chunkOnNameNode, srcDataNodeIp,srcDatanodeServerPort, chunkAbstractFromNameNode.get(chunkOnNameNode).filePath);
						if (Hdfs.Core.DEBUG) {
							System.out.format("DEBUG NameNode.SystemCheck.run(): Created CopyChunkTask<%s> from file<%s>\n", 
									((CopyChunkTask)task).getFileName(),chunkAbstractFromNameNode.get(chunkOnNameNode).filePath);
						}
						taskList.add(task);
					}
					
				} else {
					if (Hdfs.Core.DEBUG) {
						String debugInfo = String.format("DEBUG NameNode.SystemCheck.run(): chunk(%s) is MORE THAN RF. STAT: num=%d, rf=%d", chunkOnNameNode, chunkStat.replicaNum, replicaFac);
						System.out.println(debugInfo);
					}
					
					String dataNodeName = chunkStat.dataNodes.get(0);
					String tid = (new Date()).getTime() + "";
					Task task = new DeleteChunkTask(tid, chunkOnNameNode, chunkAbstractFromNameNode.get(chunkOnNameNode).filePath);
					
					List<Task> taskList = NameNode.this.dataNodeTaskTbl.get(dataNodeName);
					taskList.add(task);
				}
				
				
				/* Delete files whose at least one chunk cannot be found on any DataNode */
				for (String fileName : toDeleteFilesName) {
					NameNode.this.fileTbl.remove(fileName);
				}
			}
		}
		
		private class ChunkStatisticsForNameNode {
			public int replicaFactor;
			public String filePath;
			
			public ChunkStatisticsForNameNode(int rf, String file) {
				this.replicaFactor = rf;
				this.filePath = file;
			}
		}
		
		private class ChunkStatisticsForDataNode {
			public int replicaNum;
			private List<String> dataNodes = new ArrayList<String>();
			
			public List<String> getLocations() {
				return this.dataNodes;
			}
			
		}	
	}

	
}
