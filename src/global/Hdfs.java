package global;

/* port range: 1099 - 1148*/
public class Hdfs {
	
	public static class Common {
		
		public static boolean DEBUG = true;

		public static int REPLICA_FACTOR = 2;
		
		public static int CHUNK_SIZE = 1024 * 8;
		
		public static int WRITE_BUFF_SIZE = 512;
		public static int READ_BUFF_SIZE = 1024 * 1024;
		
		public static int PARTITION_TOLERANCE = 1000 * 60 * 2;
		
		public static String NAME_NODE_SERVICE_NAME = "NameNode"; //Unconfigurable
		
		public static String DATA_NODE_SERVICE_NAME = "DataNode"; //Unconfigurable
		
		public static int DATA_NODE_NUM = 3;
		
	}

	public static class NameNode {
		public static String nameNodeRegistryIP = /*"128.237.222.59"*/"localhost";
		public static int nameNodeRegistryPort = 1099;
		
	}
	
	public static class DataNode {
		public static int dataNodeRegistryPort = 1100;
	}

}
