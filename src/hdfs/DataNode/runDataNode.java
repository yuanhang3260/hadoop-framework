package hdfs.DataNode;

import global.Hdfs;

public class runDataNode {
	public static void main(String[] args) throws InterruptedException {
		DataNode dataNode1 = new DataNode(Hdfs.NameNode.nameNodeRegistryIP, Hdfs.NameNode.nameNodeRegistryPort, Hdfs.DataNode4.registryPort);
		dataNode1.init();
		Thread t1 = new Thread(dataNode1);
		t1.start();
		
		Thread.sleep(1000 * 5);
		DataNode dataNode2 = new DataNode(Hdfs.NameNode.nameNodeRegistryIP, Hdfs.NameNode.nameNodeRegistryPort, Hdfs.DataNode3.registryPort);
		dataNode2.init();
		Thread t2 = new Thread(dataNode2);
		t2.start();
		
		Thread.sleep(1000 * 5);
		DataNode dataNode3 = new DataNode(Hdfs.NameNode.nameNodeRegistryIP, Hdfs.NameNode.nameNodeRegistryPort, Hdfs.DataNode3.registryPort);
		dataNode3.init();
		Thread t3 = new Thread(dataNode3);
		t3.start();
		
		Thread.sleep(1000 * 5);
		DataNode dataNode4 = new DataNode(Hdfs.NameNode.nameNodeRegistryIP, Hdfs.NameNode.nameNodeRegistryPort, Hdfs.DataNode4.registryPort);
		dataNode4.init();
		Thread t4 = new Thread(dataNode4);
		t4.start();
//		
//		Thread.sleep(1000 * 5);
//		DataNode dataNode5 = new DataNode(Hdfs.NameNode.nameNodeRegistryIP, Hdfs.NameNode.nameNodeRegistryPort, Hdfs.DataNode5.registryPort);
//		dataNode5.init();
//		Thread t5 = new Thread(dataNode5);
//		t5.start();

	}
}
