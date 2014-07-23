package hdfs.namenode;

import global.Hdfs;
import global.MapReduce;
import hdfs.datanode.DataNodeRemoteInterface;
import hdfs.io.DataNodeEntry;
import hdfs.io.HDFSChunk;
import hdfs.io.HDFSFile;

import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class NameNode implements NameNodeRemoteInterface{
	volatile long chunkNaming;
	private Map<String, DataNodeAbstract> dataNodeTbl;
	private Map<String, HDFSFile> fileTbl;
	private Map<String, DataNodeRemoteInterface> dataNodeStubTbl;
	private Queue<DataNodeAbstract> selector = new ConcurrentLinkedQueue<DataNodeAbstract>();
	private NameNodeRemoteInterface nameNodeStub;
	private int port;
	private Object sysCheckSync = new Object();
	
	/*-------------------------- Constructor ---------------------*/
	/**
	 * @param The registry port of NameNode for RMI
	 */
	public NameNode(int port) {
		this.dataNodeTbl = new ConcurrentHashMap<String, DataNodeAbstract>();
		this.fileTbl = new ConcurrentHashMap<String, HDFSFile>();
		this.dataNodeStubTbl = new ConcurrentHashMap<String, DataNodeRemoteInterface>();
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
		
		Thread systemCheckThread = new Thread(new SystemCheck(true));
		systemCheckThread.start();
	}
	
	
	/*------------------------- RMI ----------------------------*/
	
	@Override
	public synchronized void heartBeat(String dataNodeName) {
		DataNodeAbstract dataNodeInfo = dataNodeTbl.get(dataNodeName);
		if (dataNodeInfo != null) {
			dataNodeInfo.latestHeartBeat = new Date();
		}
		if (!dataNodeInfo.isAvailable()) {
			dataNodeInfo.enableDataNode();
		}
	}
	
	@Override
	public String join(String ip, int port, List<String> chunkNameList) throws RemoteException {
		
		String dataNodeName = ip + ":" + port;  //TODO: change the naming method
		
		DataNodeAbstract dataNodeInfo = new DataNodeAbstract(ip, port, dataNodeName);
		
		Registry dataNodeRegistry = LocateRegistry.getRegistry(ip, port);
		
		try {
			DataNodeRemoteInterface dataNodeStub= (DataNodeRemoteInterface)dataNodeRegistry.lookup(Hdfs.Core.DATA_NODE_SERVICE_NAME);
			this.dataNodeStubTbl.put(dataNodeName, dataNodeStub);
			dataNodeTbl.put(dataNodeName, dataNodeInfo);
			selector.offer(dataNodeInfo);
			dataNodeInfo.chunkList = chunkNameList;
			if (Hdfs.Core.DEBUG) {
				System.out.format("DEBUG NameNode.join(): %s joins cluster.\n", dataNodeName);
			}
			SystemCheck oneTimeCheck = new SystemCheck(false);
			Thread th = new Thread(oneTimeCheck);
			th.start();
			return dataNodeName;
		} catch (NotBoundException e) {
			return null;
		}
	}

	@Override
	public void chunkReport(String dataNodeName, List<String> chunkList) throws RemoteException {
		DataNodeAbstract dataNodeInfo = this.dataNodeTbl.get(dataNodeName);
		dataNodeInfo.chunkList = chunkList;
	}
	
	
	@Override
	public synchronized HDFSFile create(String filePath) throws RemoteException, IOException {
		if (this.fileTbl.containsKey(filePath)) {
			if (Hdfs.Core.DEBUG) {
				System.err.format("File(%s) is duplicated", filePath);
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
	public void delete(String path) throws RemoteException, IOException {
		HDFSFile file = this.fileTbl.get(path);
		if (file == null) {
			return;
		}
		
		boolean caughtException = false;
		Exception e1 = null;
		
		for (HDFSChunk chunk : file.getChunkList()) {
			
			for (DataNodeEntry dataNode : chunk.getAllLocations()) {
				try {
					Registry dataNodeRegistry = LocateRegistry.getRegistry(dataNode.dataNodeRegistryIP, dataNode.dataNodeRegistryPort);
					DataNodeRemoteInterface dataNodeStub = (DataNodeRemoteInterface) dataNodeRegistry.lookup("DataNode");
					dataNodeStub.deleteChunk(chunk.getChunkName());
				} catch (RemoteException e) {
					this.fileTbl.remove(path);
					e1 = e;
					caughtException = true;
				} catch (NotBoundException e) {
					e1 = e;
					caughtException = true;
				} catch (IOException e) {
					e1 = e;
					caughtException = true;
				}
			}
		}
//		if (caughtException) {
//			throw new IOException("Caught exception while delete chunk.", e1);
//		}
		this.fileTbl.remove(path);
	}
	
	@Override
	public ArrayList<String> listFiles() throws RemoteException {
		Set<String> fileNameSet = this.fileTbl.keySet();
		ArrayList<String> rst = new ArrayList<String>();
		for (String fileName : fileNameSet) {
			rst.add(fileName);
		}
		return rst;
	}
	public synchronized String nameChunk() {
		String chunkName = String.format("%d", new Date().getTime());
		return chunkName;
	}
	
	public synchronized List<DataNodeEntry> select(int replicaFactor) {
		List<DataNodeEntry> rst = new ArrayList<DataNodeEntry>();
		int counter = 0;
		int hardCounter = 0; //count the traversed dataNode. It should be less than selector.size();
		while (counter < replicaFactor && counter < selector.size() && hardCounter < selector.size()) {
			DataNodeAbstract chosenDataNode = this.selector.poll();
			if (chosenDataNode.isAvailable()) {
				rst.add(new DataNodeEntry(chosenDataNode.dataNodeRegistryIP, chosenDataNode.dataNodeRegistryPort, chosenDataNode.dataNodeName));
				counter++;
			}
			hardCounter++;
			this.selector.offer(chosenDataNode);
		}
		
		if (Hdfs.Core.DEBUG) {
			ArrayList<String> list = new ArrayList<String>();
			
			for (DataNodeEntry dn : rst) {
				list.add(dn.getNodeName());
			}
			String print_rst = String.format("DEBUG NameNode.select(): %s", list.toString());
			System.out.println(print_rst);
		}
		
		return rst;
	}


	public synchronized void commitFile(HDFSFile file) throws RemoteException {

		synchronized (this.sysCheckSync) {
			
			this.fileTbl.put(file.getName(), file);
			List<HDFSChunk> totalChunkList = file.getTotalChunkList();
			List<HDFSChunk> validChunkList = file.getChunkList();
			if (Hdfs.Core.DEBUG) {
				System.out.format("DEBUG NameNode.commitFile(): Total ChunkList length = %d\n", totalChunkList.size());
			}
			
			for (HDFSChunk chunk : totalChunkList) {

				if (Hdfs.Core.DEBUG) {
					System.out.format("DEBUG NameNode.commitFile(): location num = %d \n", chunk.getAllLocations().size());
				}
				
				boolean commitAtLeastOnce = false;
				try {
					for (DataNodeEntry dn : chunk.getAllLocations()) {
						if (Hdfs.Core.DEBUG) {
							System.out.format("DEBUG NameNode.commitFile(): locations:%s\n", dn.getNodeName());
						}
						
						if (validChunkList.contains(chunk)) {
							this.dataNodeTbl.get(dn.getNodeName()).chunkList.add(chunk.getChunkName());
							this.dataNodeStubTbl.get(dn.getNodeName()).commitChunk(chunk.getChunkName(), true, false);
						} else {
							this.dataNodeStubTbl.get(dn.getNodeName()).commitChunk(chunk.getChunkName(), false, false);
						}
						commitAtLeastOnce = true;
					}
				} catch (RemoteException e) {
					
				}
				
				if (!commitAtLeastOnce) {
					if (chunk.getAllLocations() != null && chunk.getAllLocations().size() > 0) {
						throw new RemoteException("ConnectException: Connection refused to host: "
								+ chunk.getAllLocations().get(0).dataNodeRegistryIP);
					} else {
						throw new RemoteException("No DataNode available.");
					}
				}
			}
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
		private boolean available;
		private List<String> chunkList;
		private Date latestHeartBeat;
		
		public DataNodeAbstract(String ip, int port, String name) {
			this.chunkList = new ArrayList<String>();
			this.latestHeartBeat = new Date();
			this.dataNodeRegistryIP = ip;
			this.dataNodeRegistryPort = port;
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
		 * Disable the DataNode. When NameNode hasn't received the heart-beat
		 * from DataNodes because of either too long network partition or DataNode 
		 * failure, this DataNode needs to be regarded as disabled. Therefore 
		 * new replica may be created if needed.
		 */
		private void disableDataNode() {
			this.available = false;
		}
		
		/**
		 * If the NameNode receives DataNode's heart beat when the network
		 * partition has gone, the DataNode is enabled again.
		 */
		private void enableDataNode() {
			this.available = true;
		}
		
		/**
		 * Check if the DataNode is regarded as available to service.
		 * @return The availability of DataNode.
		 */
		private boolean isAvailable() {
			return this.available;
		}
		
	}
	
	/**
	 * This runnable class acts as system check 
	 * @author JeremyFu and Kim Wei
	 *
	 */
	private class SystemCheck implements Runnable {
		private boolean routineThread;
		
		public SystemCheck(boolean forever) {
			this.routineThread = forever;
		}
		
		public void run() {
			do {
				synchronized (NameNode.this.sysCheckSync) {
					
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
					for (String dataNode : NameNode.this.dataNodeTbl.keySet()) {
						DataNodeAbstract dataNodeInfo = NameNode.this.dataNodeTbl.get(dataNode);
						if ( (now.getTime() - dataNodeInfo.latestHeartBeat.getTime()) >= Hdfs.Core.PARTITION_TOLERANCE) {
							dataNodeInfo.disableDataNode();
						} else {
							if (!dataNodeInfo.isAvailable()) {
								dataNodeInfo.enableDataNode();
							}
						}
					}
					
					/* Obtain chunk abstract from name node */
					Set<String> filePaths = NameNode.this.fileTbl.keySet();
					for (String filePath : filePaths) {
						HDFSFile file = NameNode.this.fileTbl.get(filePath);
						List<HDFSChunk> chunkList = file.getChunkList();
						for (HDFSChunk chunk : chunkList) {
							chunkAbstractFromNameNode.put(chunk.getChunkName(),
									new ChunkStatisticsForNameNode(chunk.getReplicaFactor(), chunk.getChunkName()));
						}
					}
					
					/* Obtain chunk abstract from data node */
					Collection<DataNodeAbstract> dataNodes = NameNode.this.dataNodeTbl.values();
					for (DataNodeAbstract dataNode : dataNodes) {
						if (!dataNode.isAvailable()) {
							continue;
						}
						
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
					
					/* Delete orphan chunks */
					Set<String> chunksOnDataNode = chunkAbstractFromDataNode.keySet();
					if (Hdfs.Core.DEBUG) {
						System.out.println("DEBUG NameNode.SystemCheck.run(): chunks collected from data node: " + chunksOnDataNode.toString());
					}
					for (String chunkOnDataNode : chunksOnDataNode) {
						if (!chunkAbstractFromNameNode.containsKey(chunkOnDataNode)) {
							if (Hdfs.Core.DEBUG) {
								System.out.println("DEBUG NameNode.SystemCheck.run(): chunk(" + chunkOnDataNode + ") is orphan.");
							}
							ChunkStatisticsForDataNode chunkStat = chunkAbstractFromDataNode.get(chunkOnDataNode);
							for (String dataNodeName : chunkStat.dataNodes) {
								DataNodeRemoteInterface dataNodeStub = NameNode.this.dataNodeStubTbl.get(dataNodeName);
								try {
									dataNodeStub.deleteChunk(chunkOnDataNode);
								} catch (RemoteException e) {
									if (Hdfs.Core.DEBUG) {
										e.printStackTrace();
									}
								} catch (IOException e) {
									if (Hdfs.Core.DEBUG) {
										e.printStackTrace();
									}
								}
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
							DataNodeRemoteInterface srcDataNodeStub = NameNode.this.dataNodeStubTbl.get(srcDataNodeName);
							List<DataNodeEntry> dstDataNodes = NameNode.this.select(replicaFac - chunkStat.replicaNum);
							copyChunk(chunkOnNameNode, srcDataNodeStub, dstDataNodes);
						} else {
							if (Hdfs.Core.DEBUG) {
								String debugInfo = String.format("DEBUG NameNode.SystemCheck.run(): chunk(%s) is MORE THAN RF. STAT: num=%d, rf=%d", chunkOnNameNode, chunkStat.replicaNum, replicaFac);
								System.out.println(debugInfo);
							}
							String dataNodeName = chunkStat.dataNodes.get(0);
							deleteChunk(chunkOnNameNode, NameNode.this.dataNodeStubTbl.get(dataNodeName));
						}
						
						
						/* Delete files whose at least one chunk cannot be found on any DataNode */
						for (String fileName : toDeleteFilesName) {
							NameNode.this.fileTbl.remove(fileName);
						}
					}
				}
				
				

				if (Hdfs.Core.DEBUG) {
					System.out.println("DEBUG NameNode.SystemCheck.run(): Finish SystemCheck.");
				}
				try {
					Thread.sleep(Hdfs.Core.SYSTEM_CHECK_PERIOD);
				} catch (InterruptedException e) {
					if (Hdfs.Core.DEBUG) {
						e.printStackTrace();
					}
				}
			} while (routineThread);
		
			
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
		
		private void copyChunk(String chunkName, DataNodeRemoteInterface srcDataNodeStub, List<DataNodeEntry> dstDataNodes) {
			byte[] buff = null;
			int offset = 0;
			try {
				buff = srcDataNodeStub.read(chunkName, offset);
				while (buff.length > 0) {
					for (DataNodeEntry dstDataNode : dstDataNodes) {
						NameNode.this.dataNodeStubTbl.get(dstDataNode.getNodeName()).writeToLocal(buff, chunkName, offset);
					}
					offset += buff.length;
					buff = srcDataNodeStub.read(chunkName, offset);
				}
				for (DataNodeEntry dstDataNode : dstDataNodes) {
					NameNode.this.dataNodeStubTbl.get(dstDataNode.getNodeName()).commitChunk(chunkName, true, true);
				}
			} catch (IOException e) {
				if (Hdfs.Core.DEBUG) {
					e.printStackTrace();
				}
			}
		}
		
		private void deleteChunk(String chunkName, DataNodeRemoteInterface dataNodeStub) {
			try {
				if (Hdfs.Core.DEBUG) {
					System.out.format("DEBUG NameNode.SystemCheck.run(): Delete file(%s)\n", chunkName);
				}
				dataNodeStub.deleteChunk(chunkName);
			} catch (IOException e) {
				if (Hdfs.Core.DEBUG) {
					e.printStackTrace();
				}
			}
		}
		
	}
	
	
	
}
