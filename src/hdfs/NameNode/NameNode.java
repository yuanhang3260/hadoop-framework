package hdfs.NameNode;

import global.Hdfs;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class NameNode implements NameNodeRemoteInterface{
	private int dataNodeNaming;
	private ConcurrentHashMap<Integer, DataNodeInfo> dataNodeTbl;
	private ConcurrentHashMap<String, FileMetaData> metaDataTbl;
	
	public NameNode() {
		this.dataNodeTbl = new ConcurrentHashMap<Integer, DataNodeInfo>();
		this.metaDataTbl = new ConcurrentHashMap<String, FileMetaData>();
		this.dataNodeNaming = 1;
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
			System.out.println("DEBUG NameNode.heartBeat(): Heartbeat Message from Node " + dataNodeName);
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
		
		public DataNodeInfo(String ip, int port) {
			this.chunks = new ArrayList<Long>();
			this.latestHeartBeat = new Date();
		}
	}
	
	private class FileMetaData {
		private List<Long> chunks;
		private Hashtable<Long, ChunkMetaData> chunkInfoTbl;
		private int replicaFactor;  
		
	}
	
	private class ChunkMetaData {
		private long chunkName;
		private List<Integer> locations;
	}
}
