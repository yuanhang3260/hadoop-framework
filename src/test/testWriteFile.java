package test;

import hdfs.IO.HDFSOutputStream;
import hdfs.NameNode.NameNodeRemoteInterface;

import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class testWriteFile {
	public static void main (String[] args) throws RemoteException, NotBoundException {
		String nameNodeRegistryIP = "localhost";
		int nameNodeRegistryPort = 1099;
		int buff_size1 = 7;
		int buff_size2 = 9;
 		Registry nameNodeRegistry = LocateRegistry.getRegistry(nameNodeRegistryIP, nameNodeRegistryPort);
		NameNodeRemoteInterface nameNodeStub = (NameNodeRemoteInterface) nameNodeRegistry.lookup("NameNode");
		HDFSOutputStream out = nameNodeStub.create("test_tmp/test-file-4");
		if (out == null) {
			System.err.println("null out");
			System.exit(-1);
		}
		byte[] buff1 = new byte[buff_size1];
		byte[] buff2 = new byte[buff_size2];
		for (int i = 0; i < buff_size1; i++) {
			buff1[i] = (byte) ('1' + i);
		}
		for (int i = 0; i < buff_size2; i++) {
			buff2[i] = (byte) ('a' + i);
		}
		try {
			out.write(buff1);
			out.write(buff2);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
}
