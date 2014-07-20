package hdfs.DataNode;

import global.Hdfs;
import global.Parser;

public class runDataNode {
	public static void main(String[] args) throws InterruptedException {
		
//		if (args.length < 1) {
//			System.out.println("Usage:\t<DatNode Sequential>");
//		}
		
		int dataNodeSEQ = 0/*Integer.parseInt(args[0])*/;
		
		try {
			Parser.dataNodeConf(dataNodeSEQ);
			Parser.printConf(new String[] {"HDFSCommon", "NameNode"});
		} catch (Exception e) {
			e.printStackTrace();
			
			System.err.println("The DataNode rountine cannot read configuration info.\n"
					+ "Please confirm the hdfs-core.xml is placed as ./conf/hdfs-core.xml.\n"
					+ "The DataNode routine is shutting down...");
			
			System.exit(1);
		}
		
		DataNode dataNode = new DataNode(Hdfs.NameNode.nameNodeRegistryIP, Hdfs.NameNode.nameNodeRegistryPort, Hdfs.DataNode.dataNodeRegistryPort);
		dataNode.init();
		Thread t1 = new Thread(dataNode);
		t1.start();

	}
}
