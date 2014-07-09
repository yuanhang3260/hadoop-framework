package test;

import global.Hdfs;
import hdfs.IO.HDFSInputStream;
import hdfs.IO.HDFSOutputStream;
import hdfs.NameNode.NameNodeRemoteInterface;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Arrays;

public class testReadFile {
	public static void main(String[] args) throws NotBoundException, IOException {
		String nameNodeRegistryIP = "localhost";
		int nameNodeRegistryPort = Hdfs.NameNode.nameNodeRegistryPort;
 		Registry nameNodeRegistry = LocateRegistry.getRegistry(nameNodeRegistryIP, nameNodeRegistryPort);
		NameNodeRemoteInterface nameNodeStub = (NameNodeRemoteInterface) nameNodeRegistry.lookup("NameNode");
		HDFSOutputStream out = nameNodeStub.create("test-file-6");
		if (out == null) {
			System.err.println("null out");
			System.exit(-1);
		}
//		String str1 = "abc\nd\nefghi\n\nb\n12345\n567";
//		String str2 = "\nhijklm\n";
//		byte[] buff1 = str1.getBytes();
//		byte[] buff2 = str2.getBytes();
//		
//		try {
//			out.write(buff1);
//			//out.write(buff2);
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
		
		File outFile = new File("output");
		try {
			outFile.createNewFile();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		FileOutputStream fileOut = new FileOutputStream(outFile);
		HDFSInputStream in = nameNodeStub.open("largefile");
		byte[] b = new byte[2048];
		int k = 0;
		int total = 0;
		while ((k = in.read(b)) != 0) {
			if (Hdfs.DEBUG) {
				System.out.println("DEBUG testReadFile.main(): read "+ k + " bytes");
			}
			total += k;
//			for (int i = 0; i < k; i++) {
//				System.out.print(new String(new byte[]{b[i]}));
//			}
			if (k == 2048) {
				fileOut.write(b);
			} else {
				byte[] tmp_b = Arrays.copyOfRange(b, 0, k);
				fileOut.write(tmp_b);
			}
		}
		fileOut.close();
		System.out.println("Bytes read in total: " + total);
		
//		if (Hdfs.DEBUG)
//			System.out.println("DEBUG testReadFile.main(): read "+ in.read(b) + " bytes");
//		System.out.println(new String(b));
//		
//		b = new byte[3];
//		if (Hdfs.DEBUG)
//			System.out.println("DEBUG testReadFile.main(): read "+ in.read(b) + " bytes");
//		System.out.println(new String(b));
//		
//		b = new byte[3];
//		if (Hdfs.DEBUG)
//			System.out.println("DEBUG testReadFile.main(): read "+ in.read(b) + " bytes");
//		System.out.println(new String(b));
		
	}
}
