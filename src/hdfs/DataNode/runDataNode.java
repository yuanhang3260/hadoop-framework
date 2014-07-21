package hdfs.DataNode;

import global.Hdfs;
import global.Parser;
import global.Parser.ConfOpt;

public class runDataNode {
	public static void main(String[] args) throws InterruptedException {
		
		if (args == null || args.length < 1) {
			System.out.println("Usage:\t<DatNode Sequential>");
			System.exit(1);
		}
		
		int dataNodeSEQ = Integer.parseInt(args[0]);
		
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
