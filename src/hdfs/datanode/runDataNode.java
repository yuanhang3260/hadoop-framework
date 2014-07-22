package hdfs.datanode;

import java.net.UnknownHostException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;

import global.Hdfs;
import global.Parser;
import global.Parser.ConfOpt;

public class runDataNode {
	public static void main(String[] args) throws InterruptedException, RemoteException, UnknownHostException, NotBoundException {
		
		
		try {
			Parser.dataNodeConf();
			Parser.printConf(new ConfOpt[] {ConfOpt.HDFSCORE, ConfOpt.DATANODE});
		} catch (Exception e) {
			e.printStackTrace();
			
			System.err.println("The DataNode rountine cannot read configuration info.\n"
					+ "Please confirm the hdfs.xml is placed as ./conf/hdfs.xml.\n"
					+ "The DataNode routine is shutting down...");
			
			System.exit(1);
		}
		
		DataNode dataNode = new DataNode(Hdfs.Core.NAME_NODE_IP, Hdfs.Core.NAME_NODE_REGISTRY_PORT, Hdfs.DataNode.DATA_NODE_REGISTRY_PORT);
		dataNode.init();
		Thread t1 = new Thread(dataNode);
		t1.start();
		
	}
}
