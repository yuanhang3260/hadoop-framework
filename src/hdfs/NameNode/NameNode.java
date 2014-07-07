package hdfs.NameNode;

import global.Hdfs;
import hdfs.IO.HDFSOutputStream;

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
	private Map<String, FileMetaData> metaDataTbl;
	private long blocksize;
	
	public NameNode() {
		this.dataNodeTbl = new ConcurrentHashMap<Integer, DataNodeInfo>();
		this.metaDataTbl = new ConcurrentHashMap<String, FileMetaData>();
		this.dataNodeNaming = 1;
		this.chunkNaming = 1;
		this.blocksize = 4 * 1024 * 1024;
	}
	
	public void init() {
		Registry registry;
		try {
			registry = LocateRegistry.createRegistry(1099);
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
		private Hashtable<Long, ChunkMetaData> chunkInfoTbl;
		private int replicaFactor; 
		
		public FileMetaData() {
			this.chunks = new ArrayList<Long>();
			this.chunkInfoTbl = new Hashtable<Long, ChunkMetaData>();
			this.replicaFactor = 3;
		}
		
		public FileMetaData(int rf) {
			this.chunks = new ArrayList<Long>();
			this.chunkInfoTbl = new Hashtable<Long, ChunkMetaData>();
			this.replicaFactor = rf;
		}
		
		private ChunkMetaData addChunk() {
			long chunkID = NameNode.this.chunkNaming++;
			ChunkMetaData chunkMetaData = new ChunkMetaData(chunkID);
			this.chunkInfoTbl.put(chunkID, chunkMetaData);
			Set<Integer> allDataNode = NameNode.this.dataNodeTbl.keySet();
			Iterator<Integer> it = allDataNode.iterator();
			for (int i = 0; it.hasNext() && i < this.replicaFactor; i++) {
				chunkMetaData.locations.add(it.next());
			}
			return chunkMetaData;
		}
	
	}
	
	private class ChunkMetaData {
		private long chunkName;
		private List<Integer> locations;
		
		public ChunkMetaData(long chunkID) {
			this.chunkName = chunkID;
			locations = new ArrayList<Integer>();
		}
		
	}

	@Override
	public HDFSOutputStream create(String path) throws RemoteException {
		FileMetaData newFile = createFileEntry(path);
		if (newFile == null) {
			//PATH(file name) already exists
			return null;
		}
		ChunkMetaData firstChunkMetaData = newFile.addChunk();
		
		ChunkManipulationHandler handler = new ChunkManipulationHandler(firstChunkMetaData.chunkName);
		List<Integer> dataNodes = firstChunkMetaData.locations;
		for (int dataNode : dataNodes) {
			DataNodeInfo dataNodeInfo = this.dataNodeTbl.get(dataNode);
			handler.addDataNode(dataNodeInfo.dataNodeRegistryIP, dataNodeInfo.dataNodeRegistryPort);
		}
		 HDFSOutputStream out = new HDFSOutputStream((int)this.blocksize, handler);
		
		return out;
	}
	
	private FileMetaData createFileEntry (String path) {
		if (this.metaDataTbl.get(path) != null) {
			return null;
		}
		FileMetaData newFile = new FileMetaData();
		this.metaDataTbl.put(path, newFile);
		return newFile;
	}
	
	
}
