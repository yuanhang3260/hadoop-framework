package test.testHDFS;

import global.Hdfs;
import hdfs.io.HDFSFile;
import hdfs.io.HDFSInputStream;
import hdfs.io.HDFSOutputStream;
import hdfs.namenode.NameNodeRemoteInterface;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class testReadFile {
	public static void main(String[] args) throws NotBoundException, IOException {
		String fileName = "test-file-1";
		String nameNodeRegistryIP = "localhost";
		int nameNodeRegistryPort = Hdfs.Core.NAME_NODE_REGISTRY_PORT;
 		Registry nameNodeRegistry = LocateRegistry.getRegistry(nameNodeRegistryIP, nameNodeRegistryPort);
		NameNodeRemoteInterface nameNodeStub = (NameNodeRemoteInterface) nameNodeRegistry.lookup("NameNode");
		HDFSFile file = nameNodeStub.create(fileName);
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
		
		File outFile = new File("output");
		try {
			outFile.createNewFile();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		FileOutputStream fileOut = new FileOutputStream(outFile);
		
		
		file = nameNodeStub.open(fileName);
		HDFSInputStream in = file.getInputStream();
		byte[] b = new byte[2048];
		int k = 0;
		int total = 0;
		while ((k = in.read(b)) != 0) {
			if (Hdfs.Core.DEBUG) {
				System.out.println("DEBUG testReadFile.main(): read "+ k + " bytes");
			}
			total += k;
			for (int i = 0; i < k; i++) {
				System.out.print(new String(new byte[]{b[i]}));
			}
		}
		fileOut.close();
		System.out.println("Bytes read in total: " + total);
		
	}
}
