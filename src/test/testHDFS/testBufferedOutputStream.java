package test.testHDFS;

import global.Hdfs;
import hdfs.DataStructure.HDFSFile;
import hdfs.IO.HDFSBufferedOutputStream;
import hdfs.NameNode.NameNodeRemoteInterface;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class testBufferedOutputStream {
	
	public static void main(String[] args) throws IOException, NotBoundException {
		File file1 = new File("./test_file/buff_0");
		File file2 = new File("./test_file/buff_1");
		
		if (!file1.exists()) {
			file1.createNewFile();
		}
		
		if (!file2.exists()) {
			file2.createNewFile();
		}
		
		String str1 = "abcdefghijklmn";
		String str2 = "1234567";
		byte[] buff1 = str1.getBytes();
		byte[] buff2 = str2.getBytes();
		
		FileOutputStream fout = new FileOutputStream(file1);
		
		Registry nameNodeR = LocateRegistry.getRegistry(Hdfs.Core.NAME_NODE_IP, Hdfs.Core.NAME_NODE_REGISTRY_PORT);
		NameNodeRemoteInterface nameNodeI = (NameNodeRemoteInterface) nameNodeR.lookup(Hdfs.Core.NAME_NODE_SERVICE_NAME);
		HDFSFile file = nameNodeI.create("test");
		HDFSBufferedOutputStream hout = new HDFSBufferedOutputStream(file.getNewOutputStream());
		
		int len1 = 4;
		fout.write(buff1, 0, len1);
		hout.write(buff1, 0, len1);
		fout.write(buff1, len1, buff1.length - len1);
		hout.write(buff1, len1, buff1.length - len1);
		
		fout.write(buff2, 0, buff2.length);
		hout.write(buff2, 0, buff2.length);
		
		hout.close();
		fout.close();
	
	}
}
