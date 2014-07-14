package test.testHDFS;

import hdfs.NameNode.NameNodeRemoteInterface;

import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class testDeleteFile {
	public static void main (String[] args) throws RemoteException, NotBoundException {
		String nameNodeRegistryIP = "localhost";
		int nameNodeRegistryPort = 1099;
 		Registry nameNodeRegistry = LocateRegistry.getRegistry(nameNodeRegistryIP, nameNodeRegistryPort);
		NameNodeRemoteInterface nameNodeStub = (NameNodeRemoteInterface) nameNodeRegistry.lookup("NameNode");
		try {
			nameNodeStub.delete("test-file");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
