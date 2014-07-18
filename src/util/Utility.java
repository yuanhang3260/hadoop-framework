package util;

import global.Hdfs;
import hdfs.DataStructure.HDFSFile;
import hdfs.IO.HDFSInputStream;
import hdfs.IO.HDFSOutputStream;
import hdfs.NameNode.NameNodeRemoteInterface;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.Arrays;

public class Utility {
	
	public static void main(String[] args) {
		if (args.length < 2) {
			printUsage();
			return;
		}
		
		if (!args[0].equals("hadoop")) {
			printUsage();
			return;
		}
		
		if (args[1].equals("put")) {
			if (args.length < 4) {
				printPutUsage();
				return;
			}
			String localFilePath = args[2];
			String hdfsFilePath  = args[3];
			putToHDFS(localFilePath, hdfsFilePath);
		} else if (args[1].equals("get")) {
			if (args.length < 4) {
				printGetUsage();
				return;
			}
			String localFilePath = args[3];
			String hdfsFilePath  = args[2];
			getFromHDFS(hdfsFilePath, localFilePath);
		} else if (args[1].equals("rm")) {
			if (args.length < 3) {
				printRmUsage();
				return;
			}
			String hdfsFilePath = args[2];
			removeFromHDFS(hdfsFilePath);
		} else if (args[1].equals("ls")) {
			listFiles();
			return;
		} else {
			printUsage();
			return;
		}
	}
	
	private static void getFromHDFS(String hdfsFilePath, String localFilePath) {
		try {
			Registry nameNodeRegistry = LocateRegistry.getRegistry(Hdfs.NameNode.nameNodeRegistryIP, Hdfs.NameNode.nameNodeRegistryPort);
			NameNodeRemoteInterface nameNodeStub = (NameNodeRemoteInterface) nameNodeRegistry.lookup("NameNode");
			
			HDFSFile file = nameNodeStub.open(hdfsFilePath);
			if (file == null) {
				System.out.println("Error! HDFS file does not exists");
				System.exit(-1);
			}
			HDFSInputStream in = file.getInputStream();
			int c = 0;
			int buff_len = Hdfs.Client.READ_BUFFER_SIZE;
			byte[] buff = new byte[buff_len];
			File newFile = new File(localFilePath);
			FileOutputStream out = null;
			try {
				out =  new FileOutputStream(newFile);
			} catch (FileNotFoundException e) {
				try {
					newFile.createNewFile();
					out =  new FileOutputStream(newFile);
				} catch (IOException e1) {
					System.out.println("Error! Failed to put file to HDFS.");
					System.exit(-1);
				}
			}
			int counter = 0;
			while ((c = in.read(buff)) != 0) {
				out.write(buff, 0, c);
				counter += c;
			}
			out.close();
			System.out.println("TOTALLY READ: " + counter);
		} catch (RemoteException e) {
			System.out.println("Error! Failed to put file to HDFS.");
			System.exit(-1);
		} catch (NotBoundException e) {
			System.out.println("Error! Failed to put file to HDFS.");
			System.exit(-1);
		} catch (IOException e) {
			System.out.println("Error! Failed to put file to HDFS.");
			System.exit(-1);
		}
	}
	
	private static void putToHDFS(String localFilePath, String hdfsFilePath) {
		File newFile = new File(localFilePath);
		if (!newFile.exists()) {
			System.out.println("Error! Local file does not exists");
			System.exit(-1);
		}
		
		byte[] buff = new byte[Hdfs.WRITE_BUFF_SIZE];
		try {
			FileInputStream in = new FileInputStream(newFile);
			int readBytes;
			Registry nameNodeRegistry = LocateRegistry.getRegistry(Hdfs.NameNode.nameNodeRegistryIP, Hdfs.NameNode.nameNodeRegistryPort);
			NameNodeRemoteInterface nameNodeStub = (NameNodeRemoteInterface) nameNodeRegistry.lookup("NameNode");
			
			HDFSFile file = nameNodeStub.create(hdfsFilePath);
			if (file == null) {
				System.out.println("Error! File name has been used.");
				System.exit(-1);
			}
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
	
	private static void removeFromHDFS(String path) {
		try {
			Registry nameNodeRegistry = LocateRegistry.getRegistry(Hdfs.NameNode.nameNodeRegistryIP, Hdfs.NameNode.nameNodeRegistryPort);
			NameNodeRemoteInterface nameNodeStub = (NameNodeRemoteInterface) nameNodeRegistry.lookup(Hdfs.NameNode.nameNodeServiceName);
			nameNodeStub.delete(path);
		} catch (RemoteException e){
			System.out.println("Remove operation may failed");
			System.exit(-1);
		} catch (NotBoundException e) {
			System.out.println("Error! Cannot find name node.");
			System.exit(-1);
		} catch (IOException e) {
			System.out.println("Exception! Some data node may lose connection. The file is removed.");
			System.exit(-1);
		}
	}
	
	private static void listFiles() {
		try {
			Registry nameNodeRegistry = LocateRegistry.getRegistry(Hdfs.NameNode.nameNodeRegistryIP, Hdfs.NameNode.nameNodeRegistryPort);
			NameNodeRemoteInterface nameNodeStub = (NameNodeRemoteInterface) nameNodeRegistry.lookup(Hdfs.NameNode.nameNodeServiceName);
			ArrayList<String> rst = nameNodeStub.listFiles();
			int i = 1;
			for (String fileName : rst) {
				System.out.format("%d\t%s\n", i, fileName);
				i++;
			}
			if (rst.size() < 1) {
				System.out.println("No files on HDFS");
			}
		} catch (RemoteException e){
			System.out.println("Error! Cannot find name node.");
			System.exit(-1);
		} catch (NotBoundException e) {
			System.out.println("Error! Cannot find name node.");
			System.exit(-1);
		}
	}
	
	public static void printUsage() {
		System.out.println("Usage:\thadoop <op> \n<op>:\n\tput\n\tget");
	}
	
	private static void printPutUsage() {
		System.out.println("Usage:\thadoop\tput\t<src file name>\t<dst file name>");
	}
	
	private static void printGetUsage() {
		System.out.println("Usage:\thadoop\tget\t<dst file name>\t<src file name>");
	}
	
	private static void printRmUsage() {
		System.out.println("Usage:\thadoop\tdelete\t<obj file name>");
	}
}
