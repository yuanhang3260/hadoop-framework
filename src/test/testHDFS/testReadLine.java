package test.testHDFS;

import global.Hdfs;
import global.Parser;
import hdfs.DataNode.DataNodeRemoteInterface;
import hdfs.io.HDFSLineFeedCheck;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class testReadLine {
	
	public static void main(String[] args) {
		
		try {
			Parser.hdfsCoreConf();
			Parser.dataNodeConf();
			
			String chunkName = "1405889958420";
			
			Registry dataNodeR = LocateRegistry.getRegistry("localhost", Hdfs.DataNode.DATA_NODE_REGISTRY_PORT);
			DataNodeRemoteInterface dataNodeS = (DataNodeRemoteInterface) dataNodeR.lookup(Hdfs.Core.DATA_NODE_SERVICE_NAME);

			HDFSLineFeedCheck rst = dataNodeS.readLine(chunkName);
			System.out.println("Is rst.line == null?" + rst.line == null);
			System.out.println("readline:" +rst.line + "\t Met line feed:" + rst.metLineFeed);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
	
}
