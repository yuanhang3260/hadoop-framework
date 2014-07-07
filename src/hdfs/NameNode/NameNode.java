package hdfs.NameNode;

import global.Hdfs;
import hdfs.IO.HDFSInputStream;
import hdfs.IO.HDFSOutputStream;

import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class NameNode implements NameNodeRemoteInterface{
	private int dataNodeNaming;
	private long chunkNaming;
	private Map<Integer, DataNodeInfo> dataNodeTbl;
	private Map<String, FileMetaData> fileMetaDataTbl;
	private long chunksize;
	private int registryPort;
	
	public NameNode() {
		this.dataNodeTbl = new ConcurrentHashMap<Integer, DataNodeInfo>();
		this.fileMetaDataTbl = new ConcurrentHashMap<String, FileMetaData>();
		this.dataNodeNaming = 1;
		this.chunkNaming = 1;
		this.chunksize = 7;
		this.registryPort = 1099;
	}
	
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
	public synchronized void heartBeat(int dataNodeName) {
		if (Hdfs.DEBUG) {
			System.out.println("DEBUG NameNode.heartBeat(): Heartbeat Message from Node-" + dataNodeName);
		}	
		DataNodeInfo dataNodeInfo = dataNodeTbl.get(dataNodeName);
		if (dataNodeInfo != null) {
			dataNodeInfo.latestHeartBeat = new Date();
		}
	}
	
	public int join(String ip, int port) {
		//TODO:get Data Node stub and save it in DataNodeInfo
		DataNodeInfo dataNodeInfo = new DataNodeInfo(ip, port);
		int dataNodeName = dataNodeNaming;
		dataNodeTbl.put(dataNodeName, dataNodeInfo);
		dataNodeNaming++;
		return dataNodeName;
	}

	@Override
	public void blockReport() {
		
	}

	@Override
	public HDFSOutputStream create(String path) throws RemoteException {
		FileMetaData newFile = createFileEntry(path);
		if (newFile == null) {
			//PATH(file name) already exists
			return null;
		}
		hdfs.NameNode.NameNode.FileMetaData.ChunkMetaData firstChunkMetaData = newFile.addChunk();
		
		ChunkInfo handler = new ChunkInfo(firstChunkMetaData.chunkName);
		List<Integer> dataNodes = firstChunkMetaData.locations;
		for (int dataNode : dataNodes) {
			if (Hdfs.DEBUG) {
				System.out.println("dataNode id=" + dataNode);
			}
			DataNodeInfo dataNodeInfo = this.dataNodeTbl.get(dataNode);
			handler.addDataNode(dataNodeInfo.dataNodeRegistryIP, dataNodeInfo.dataNodeRegistryPort);
		}
		String hostIP = null;
		try {
			hostIP = Inet4Address.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			e.printStackTrace();
			throw new RemoteException("Unknown Host");
		}
		HDFSOutputStream out = new HDFSOutputStream((int)this.chunksize, handler, path, hostIP, this.registryPort);
		if (Hdfs.DEBUG) {
			System.out.println("DEBUG NameNode.create(): Invocate create.");
		}
		return out;
	}
	
	public HDFSInputStream open(String path) throws RemoteException{
		FileMetaData fileMetaData = this.fileMetaDataTbl.get(path);
		HDFSInputStream in = null;
		if (fileMetaData != null) {
			List<ChunkInfo> chunksInfo = new ArrayList<ChunkInfo>();
			for (long chunkName : fileMetaData.chunks) {
				
			}
			
		}
		
	}
	
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
		hdfs.NameNode.NameNode.FileMetaData.ChunkMetaData newChunkMetaData = fileMetaData.addChunk();
		ChunkInfo handler = new ChunkInfo(newChunkMetaData.chunkName);
		List<Integer> dataNodes = newChunkMetaData.locations;
		for (int dataNode : dataNodes) {
			DataNodeInfo dataNodeInfo = this.dataNodeTbl.get(dataNode);
			handler.addDataNode(dataNodeInfo.dataNodeRegistryIP, dataNodeInfo.dataNodeRegistryPort);
		}
		return handler;
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
		private List<Long> chunks;
		private Hashtable<Long, ChunkMetaData> chunkMetaDataTbl;
		private int replicaFactor;  
		public FileMetaData() {
			this.chunks = new ArrayList<Long>();
			this.chunkMetaDataTbl = new Hashtable<Long, ChunkMetaData>();
			this.replicaFactor = 3;
		}
		
//		public FileMetaData(int rf) {
//			this.chunks = new ArrayList<Long>();
//			this.chunkInfoTbl = new Hashtable<Long, ChunkMetaData>();
//			this.replicaFactor = rf;
//		}
		
		private ChunkMetaData addChunk() {
			long chunkID = NameNode.this.chunkNaming++;
			ChunkMetaData chunkMetaData = new ChunkMetaData(chunkID);
			this.chunkMetaDataTbl.put(chunkID, chunkMetaData);
			Set<Integer> allDataNode = NameNode.this.dataNodeTbl.keySet();
			Iterator<Integer> it = allDataNode.iterator();
			for (int i = 0; it.hasNext() && i < this.replicaFactor; i++) {
				chunkMetaData.locations.add(it.next());
			}
			return chunkMetaData;
		}
		
		private class ChunkMetaData {
			private long chunkName;
			private List<Integer> locations;
			
			public ChunkMetaData(long chunkID) {
				this.chunkName = chunkID;
				locations = new ArrayList<Integer>();
			}
		}
	
	}
	
}
