package test.testHDFS;

import global.Hdfs;
import global.Parser;
import hdfs.NameNode.NameNodeRemoteInterface;
import hdfs.io.HDFSFile;
import hdfs.io.HDFSOutputStream;

import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class testNewOutputStream {
	public static void main (String[] args) throws NotBoundException, IOException {
		
		try {
			Parser.hdfsCoreConf();
		} catch (Exception e1) {
			e1.printStackTrace();
			System.exit(1);
		}
		
 		Registry nameNodeRegistry = LocateRegistry.getRegistry(Hdfs.Core.NAME_NODE_IP, Hdfs.Core.NAME_NODE_REGISTRY_PORT);
		NameNodeRemoteInterface nameNodeStub = (NameNodeRemoteInterface) nameNodeRegistry.lookup(Hdfs.Core.NAME_NODE_SERVICE_NAME);
		
		HDFSFile file = nameNodeStub.create("test-file-1");
		HDFSOutputStream newout = file.getOutputStream();
		
		String str1 = "abcde123\n\n";
		String str2 = "fghij\n67890";
		byte[] buff1 = str1.getBytes();
		byte[] buff2 = str2.getBytes();
		
		try {
			newout.write(buff1);
			newout.write(buff2);
			newout.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
