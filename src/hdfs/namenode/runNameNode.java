package hdfs.namenode;

import global.Hdfs;
import global.Parser;
import global.Parser.ConfOpt;

import java.rmi.RemoteException;

public class runNameNode {
	public static void main(String[] args) {
		try {
			Parser.hdfsCoreConf();
			Parser.printConf(new ConfOpt[] {ConfOpt.HDFSCORE});
		} catch (Exception e) {
			e.printStackTrace();
			
			System.err.println("The NameNode rountine cannot read configuration info.\n"
					+ "Please confirm the hdfs.xml is placed as ./conf/hdfs.xml.\n"
					+ "The NameNode routine is shutting down...");
			
			System.exit(1);
		}
		
		NameNode nameNode = new NameNode(Hdfs.Core.NAME_NODE_REGISTRY_PORT);
		try {
			nameNode.init();
		} catch (RemoteException e) {
			System.err.println("Name node cannot export and bind its remote object.\n"
					+ "The NameNode routine is shutting down...");
			System.exit(1);
		}
		if (Hdfs.Core.DEBUG) {
			System.out.println("DEBUG runNameNode.main(): NameNode now is running");
		}
		
	}
}
