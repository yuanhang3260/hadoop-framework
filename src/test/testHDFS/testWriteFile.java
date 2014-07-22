package test.testHDFS;

import hdfs.NameNode.NameNodeRemoteInterface;
import hdfs.io.HDFSFile;
import hdfs.io.HDFSOutputStream;

import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class testWriteFile {
	public static void main (String[] args) throws NotBoundException, IOException {
		String nameNodeRegistryIP = "128.237.222.59";
		int nameNodeRegistryPort = 1099;
 		Registry nameNodeRegistry = LocateRegistry.getRegistry(nameNodeRegistryIP, nameNodeRegistryPort);
		NameNodeRemoteInterface nameNodeStub = (NameNodeRemoteInterface) nameNodeRegistry.lookup("NameNode");
		HDFSFile file = nameNodeStub.create("test-file-1");
		HDFSOutputStream out = file.getOutputStream();
		if (out == null) {
			System.err.println("null out");
			System.exit(-1);
		}

		String str1 = "abc\nd\nefghi\n\nb\n12345\n567";
		String str2 = "\nhijklm\n";
		byte[] buff1 = str1.getBytes();
		byte[] buff2 = str2.getBytes();
		
		try {
			out.write(buff1);
			out.write(buff2);
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
