package hdfs.DataNode;

import global.Hdfs;

public class runDataNode {
	public static void main(String[] args) throws InterruptedException {
		DataNode dataNode1 = new DataNode(Hdfs.NameNode.nameNodeRegistryIP, Hdfs.NameNode.nameNodeRegistryPort, Hdfs.DataNode1.registryPort);
		dataNode1.init();
		Thread t1 = new Thread(dataNode1);
		t1.start();
		
//		DataNode dataNode2 = new DataNode(Hdfs.NameNode.nameNodeRegistryIP, Hdfs.NameNode.nameNodeRegistryPort, Hdfs.DataNode2.registryPort);
//		dataNode2.init();
//		Thread t2 = new Thread(dataNode2);
//		t2.start();
//		
//		DataNode dataNode3 = new DataNode(Hdfs.NameNode.nameNodeRegistryIP, Hdfs.NameNode.nameNodeRegistryPort, Hdfs.DataNode3.registryPort);
//		dataNode3.init();
//		Thread t3 = new Thread(dataNode3);
//		t3.start();
	}
}
