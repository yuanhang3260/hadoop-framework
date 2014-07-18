package hdfs.NameNode;

import global.Hdfs;
import global.Parser;

import java.rmi.RemoteException;

public class runNameNode {
	public static void main(String[] args) {
		try {
			Parser.nameNodeParseConf();
		} catch (Exception e) {
			System.err.println("The NameNode rountine cannot read configuration info.\n"
					+ "Please confirm the hdfs.core.xml is placed as ./conf/hdfs.core.xml.\n"
					+ "The NameNode routine is shutting down...");
		}
		
		NameNode nameNode = new NameNode(Hdfs.NameNode.nameNodeRegistryPort);
		try {
			nameNode.init();
		} catch (RemoteException e) {
			System.err.println("Name node cannot export and bind its remote object.\n"
					+ "The NameNode routine is shutting down...");
			System.exit(1);
		}
		if (Hdfs.DEBUG) {
			System.out.println("DEBUG runNameNode.main(): NameNode now is running");
		}
		
	}
}
