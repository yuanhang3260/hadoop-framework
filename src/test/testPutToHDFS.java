package test;

import global.Hdfs;
import hdfs.IO.HDFSOutputStream;
import hdfs.NameNode.NameNodeRemoteInterface;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Arrays;

public class testPutToHDFS {
	public static void main(String[] args) throws IOException, NotBoundException {
		File newFile = new File("largefile");
		System.out.println(newFile.getAbsolutePath());
		byte[] buff = new byte[1024];
		FileInputStream in = new FileInputStream(newFile);
		int readBytes = 0;
		Registry nameNodeRegistry = LocateRegistry.getRegistry(Hdfs.NameNode.nameNodeRegistryIP, Hdfs.NameNode.nameNodeRegistryPort);
		NameNodeRemoteInterface nameNodeStub = (NameNodeRemoteInterface) nameNodeRegistry.lookup("NameNode");
		
		HDFSOutputStream out = nameNodeStub.create("largefile");
		while ((readBytes = in.read(buff)) != -1) {
			if (readBytes == 1024) {
				out.write(buff);
			} else {
				byte[] tmp_buff = Arrays.copyOfRange(buff, 0, readBytes);
				out.write(tmp_buff);
			}
		}
		out.close();
		in.close();
	}
}
