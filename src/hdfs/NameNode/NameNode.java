package hdfs.NameNode;

import global.Hdfs;
import hdfs.DataStructure.ChunkInfo;
import hdfs.IO.HDFSOutputStream;

import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class NameNode implements NameNodeRemoteInterface{
	private int dataNodeNaming;
	private long chunkNaming;
	private Map<String, DataNodeInfo> dataNodeTbl;
	private Map<String, FileMetaData> fileMetaDataTbl;
	private long chunksize;
	private int registryPort;
	
	public NameNode(int port) {
		this.dataNodeTbl = new ConcurrentHashMap<String, DataNodeInfo>();
		this.fileMetaDataTbl = new ConcurrentHashMap<String, FileMetaData>();
		this.dataNodeNaming = 1;
		this.chunkNaming = 1;
		this.chunksize = 7;
		this.registryPort = port;
	}
	
	/* Export and bind NameNode remote object */
	public void init() {
		Registry registry;
		try {
			registry = LocateRegistry.createRegistry(this.registryPort);
			NameNodeRemoteInterface nameNodeStub = (NameNodeRemoteInterface) UnicastRemoteObject.exportObject(this, 0);
			registry.rebind("NameNode", nameNodeStub);
		} catch (RemoteException e) {
			e.printStackTrace();
		}	

	}
	
	@Override
	public synchronized void heartBeat(String dataNodeName) {
		if (Hdfs.DEBUG) {
			System.out.println("DEBUG NameNode.heartBeat(): Heartbeat Message from " + dataNodeName);
		}	
		DataNodeInfo dataNodeInfo = dataNodeTbl.get(dataNodeName);
		if (dataNodeInfo != null) {
			dataNodeInfo.latestHeartBeat = new Date();
		}
	}
	
	public String join(String ip, int port) {
		//TODO:get Data Node stub and save it in DataNodeInfo
		DataNodeInfo dataNodeInfo = new DataNodeInfo(ip, port);
		String dataNodeName = String.format("DataNode-%04d", dataNodeNaming++);
		dataNodeTbl.put(dataNodeName, dataNodeInfo);
		if (Hdfs.DEBUG) {
			System.out.println("DEBUG NameNode.join(): " + dataNodeName + " joins cluster.");
		}
		return dataNodeName;
	}

	@Override
	public void chunkReport() {
		
	}
	
	@Override
	public HDFSOutputStream create(String filePath) throws RemoteException {
		FileMetaData newFile = createFileEntry(filePath);
		if (newFile == null) {
			/* PATH(file name) already exists */
			return null;
		}
		ChunkInfo firstChunk = newFile.addChunk();
		String hostIP = null;
		try {
			hostIP = Inet4Address.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			e.printStackTrace();
			throw new RemoteException("Unknown Host");
		}
		HDFSOutputStream out = new HDFSOutputStream((int)this.chunksize, firstChunk, filePath, hostIP, this.registryPort);
		if (Hdfs.DEBUG) {
			System.out.println("DEBUG NameNode.create(): Invocate create.");
		}
		return out;
	}
	
	@Override
	public List<ChunkInfo> getFileChunks(String file) throws RemoteException {
		FileMetaData fileMetaData = this.fileMetaDataTbl.get(file);
		return fileMetaData.chunkList;
	}
	
	private String nameChunk() {
		String chunkName = String.format("%010d", this.chunkNaming++);
		return chunkName;
	}
	
	/**
	 * This is called by NameNode.create()
	 * In order to create a file, insert a file entry at NameNode first.
	 * @param path
	 * @return
	 */
	private FileMetaData createFileEntry (String path) {
		if (this.fileMetaDataTbl.get(path) != null) {
			return null;
		}
		FileMetaData newFile = new FileMetaData();
		this.fileMetaDataTbl.put(path, newFile);
		return newFile;
	}

	@Override
	public ChunkInfo applyForNewChunk(String path) throws RemoteException {
		FileMetaData fileMetaData = this.fileMetaDataTbl.get(path);
		ChunkInfo newChunk = fileMetaData.addChunk();
		return newChunk;
	}
	
	private class DataNodeInfo {

		//TODO:a dataNode stub variable
		private Date latestHeartBeat;
		private List<Long> chunks;
		private String dataNodeRegistryIP;
		private int dataNodeRegistryPort;
		
		public DataNodeInfo(String ip, int port) {
			this.chunks = new ArrayList<Long>();
			this.latestHeartBeat = new Date();
			this.dataNodeRegistryIP = ip;
			this.dataNodeRegistryPort = port;
		}
	}
	
	private class FileMetaData {
		private List<ChunkInfo> chunkList; //Indicate chunk order
		private int replicaFactor; 
		
		public FileMetaData() {
			this.chunkList = new ArrayList<ChunkInfo>();
			this.replicaFactor = 3;
		}
		
		public ChunkInfo getChunkByName(String chunkName) {
			for (ChunkInfo chunk : this.chunkList) {
				if (chunk.getChunkName().equals(chunkName)) {
					return chunk;
				}
			}
			return null;
		}
		
		private ChunkInfo addChunk() {
			String chunkName = NameNode.this.nameChunk();
			ChunkInfo newChunk = new ChunkInfo(chunkName);
			this.chunkList.add(newChunk);
			
			/* Allocate DataNode to store this chunk */
			Set<String> allDataNodes = NameNode.this.dataNodeTbl.keySet();
			Iterator<String> it = allDataNodes.iterator();
			for (int i = 0; it.hasNext() && i < this.replicaFactor; i++) {
				String dataNodeName = it.next();
				DataNodeInfo dataNode = NameNode.this.dataNodeTbl.get(dataNodeName);
				newChunk.addDataNode(dataNode.dataNodeRegistryIP, dataNode.dataNodeRegistryPort);
			}
			return newChunk;
		}
	
	}
	
}
