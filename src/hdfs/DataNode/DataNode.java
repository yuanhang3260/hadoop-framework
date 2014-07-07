package hdfs.DataNode;

import hdfs.NameNode.NameNodeRemoteInterface;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

public class DataNode implements DataNodeRemoteInterface, Runnable{
	//TODO:Use XML to configure
	private String nameNodeIp;
	private int nameNodePort;
	private String dataNodeName;
	private int dataNodePort;
	private String dirPrefix;
	
	public DataNode(String nameNodeIp, int nameNodePort, int dataNodePort) {
		/* Name Node's RMI registry's address */
		this.nameNodeIp = nameNodeIp;
		this.nameNodePort = nameNodePort;
		this.dataNodePort = dataNodePort;
		this.dirPrefix = "test_tmp/";
	}
	
	/**
	 * Initialize this Data Node by binding the remote object
	 */
	public void init() {
		try {
			Registry localRegistry = LocateRegistry.createRegistry(this.dataNodePort);
			DataNodeRemoteInterface dataNodeStub = (DataNodeRemoteInterface) UnicastRemoteObject.exportObject(this, 0);
			localRegistry.rebind("DataNode", dataNodeStub);
		} catch (RemoteException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void run() {
		try {			
			/* join hdfs cluster */
			Registry registryOnNameNode = LocateRegistry.getRegistry(nameNodeIp, nameNodePort);
			NameNodeRemoteInterface nameNode = (NameNodeRemoteInterface) registryOnNameNode.lookup("NameNode");
			this.dataNodeName = nameNode.join(InetAddress.getLocalHost().getHostAddress(), this.dataNodePort);
			
			while (true) {
				Thread.sleep(10000);
				nameNode.heartBeat(dataNodeName);
			}
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NotBoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	@Override
	public void write(byte[] b, String chunkName, int offset) throws RemoteException {
		File chunkFile = new File(chunkNameWrapper(chunkName));
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
	
	public void modifyChunkPermission(String globalChunkName) throws RemoteException {
		File chunkFile = new File(chunkNameWrapper(globalChunkName));
		if (!chunkFile.exists()) {
			try {
				chunkFile.createNewFile();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		chunkFile.setWritable(false, false);
		return;
	}
	
	private String chunkNameWrapper(String globalChunkName) {
		return this.dirPrefix + this.dataNodeName + "-chunk-" + globalChunkName;
	}
	
//	private String chunkNameUnwrapper(String localChunkName) {
//		if (localChunkName == null) {
//			return null;
//		}
//		String[] array = localChunkName.split("-");
//		return array[2];
//	}
	
}
