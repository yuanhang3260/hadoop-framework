package hdfs.NameNode;

import global.Hdfs;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

public class runNameNode {
	public static boolean DEBUG = true;
	public static void main(String[] args) {
		NameNode nameNode = new NameNode();
		nameNode.init();
		if (Hdfs.DEBUG) {
			System.out.println("DEBUG runNameNode.main(): Name Node Server now running");
		}
	}
}
