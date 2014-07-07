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
 		Registry nameNodeRegistry = LocateRegistry.getRegistry(nameNodeRegistryIP, nameNodeRegistryPort);
		NameNodeRemoteInterface nameNodeStub = (NameNodeRemoteInterface) nameNodeRegistry.lookup("NameNode");
		HDFSOutputStream out = nameNodeStub.create("test-file-19");
		if (out == null) {
			System.err.println("null out");
			System.exit(-1);
		}
		String str1 = "abcdefg123\n";
		String str2 = "\nhijklm\n";
		byte[] buff1 = str1.getBytes();
		byte[] buff2 = str2.getBytes();
		
		try {
			out.write(buff1);
			out.write(buff2);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
}
