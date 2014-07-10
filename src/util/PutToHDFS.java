package util;

import global.Hdfs;
import hdfs.DataStructure.HDFSFile;
import hdfs.IO.HDFSOutputStream;
import hdfs.NameNode.NameNodeRemoteInterface;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Arrays;

public class PutToHDFS {
	public static void main(String[] args) throws IOException, NotBoundException {
		if (args.length != 3) {
			printUsage();
			System.exit(-1);
		}
		String filePath = args[2];
		File newFile = new File(filePath);
		if (!newFile.exists()) {
			System.out.println("Error! File does not exists");
			System.exit(-1);
		}
		
		byte[] buff = new byte[1024];
		try {
			FileInputStream in = new FileInputStream(newFile);
			int readBytes;
			Registry nameNodeRegistry = LocateRegistry.getRegistry(Hdfs.NameNode.nameNodeRegistryIP, Hdfs.NameNode.nameNodeRegistryPort);
			NameNodeRemoteInterface nameNodeStub = (NameNodeRemoteInterface) nameNodeRegistry.lookup("NameNode");
			
			HDFSFile file = nameNodeStub.create(filePath);
			HDFSOutputStream out = file.getOutputStream();
			
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
		} catch (Exception e) {
			System.out.println("Error! Failed to put file to HDFS.");
			System.exit(-1);
		}
	}
	
	public static void printUsage() {
		System.out.println("Usage:\thdfs put <filename>");
	}
	
}
