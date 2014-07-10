package hdfs.DataNode;

import global.Hdfs;
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
import java.util.ArrayList;
import java.util.List;

public class DataNode implements DataNodeRemoteInterface, Runnable{
	//TODO:Use XML to configure
	private String nameNodeIp;
	private int nameNodePort;
	private String dataNodeName;
	private int dataNodePort;
	private String dirPrefix;
	private int chunkBlockPeriod = 3;
	
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
			
			this.dataNodeName = InetAddress.getLocalHost().getHostAddress() + ":" + this.dataNodePort;
			List<String> chunkList = formChunkReport();
			if (Hdfs.DEBUG) {
				System.out.println("DEBUG DataNode.run(): " + dataNodeName + " is reporting chunks " + chunkList.toString());
			}
			
			this.dataNodeName = nameNode.join(InetAddress.getLocalHost().getHostAddress(), this.dataNodePort, chunkList);
			
			int counter = 1;

			while (true) {
				if (counter % this.chunkBlockPeriod ==  0) {
					chunkList = formChunkReport();
					if (Hdfs.DEBUG) {
						System.out.println("DEBUG DataNode.run(): " + dataNodeName + " is reporting chunks " + chunkList.toString());
					}
					nameNode.chunkReport(dataNodeName, chunkList);
				} else {
					nameNode.heartBeat(dataNodeName);
				}
				counter++;
				Thread.sleep(10000);
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
	
	/**
	 * Read the chunk data stores on this data node and return a buffer, the
	 * size of the buffer is set in global.Hdfs.client
	 * 
	 * @return a byte array with the chunk data, 
	 * 		   at most Hdfs.client.readBufSize
	 */
	public byte[] read(String chunkName, int offSet) {
		File chunkFile = new File("test_tmp/" + this.dataNodeName + "-chunk-" + chunkName);
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
		
		if (offSet + Hdfs.Client.READ_BUFFER_SIZE <= chunkSize) {
			readBuf = new byte[Hdfs.Client.READ_BUFFER_SIZE];
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
	
	@Override
	public void deleteChunk(String globalChunkName) throws RemoteException, IOException {
		try {
			File chunkFile = new File(chunkNameWrapper(globalChunkName));
			System.out.println("Delete file " + chunkFile);
			chunkFile.delete();
		} catch (SecurityException e) {
			throw new IOException("Cannot delete the chunk");
		}
		
	}
	
	private String chunkNameWrapper(String globalChunkName) {
		return this.dirPrefix + this.dataNodeName + "-chunk-" + globalChunkName;
	}
	
	public List<String> formChunkReport() {
		List<String> chunkList = new ArrayList<String>();
		File dfDir = new File(this.dirPrefix);
		String[] files = dfDir.list();
	
		for (String localFileName : files) {
			String[] segs = localFileName.split("-");
			if (segs.length == 3 && segs[0].equals(this.dataNodeName)) {
				chunkList.add(segs[2]);
			}
		}
		return chunkList;
	}
	
}
