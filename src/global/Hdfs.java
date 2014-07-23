package global;

/* port range: 1099 - 1148*/
public class Hdfs {
	
	public static class Core {
		
		public static boolean DEBUG = false; //non-configurable

		public static int REPLICA_FACTOR;
		
		public static int CHUNK_SIZE;
		
		public static int WRITE_BUFF_SIZE;
		public static int READ_BUFF_SIZE;
		
		public static int PARTITION_TOLERANCE;
		
		public static String NAME_NODE_SERVICE_NAME = "NameNode"; //non-configurable
		
		public static String DATA_NODE_SERVICE_NAME = "DataNode"; //non-configurable
		
		public static String NAME_NODE_IP;
		
		public static int NAME_NODE_REGISTRY_PORT;
		
		public static String HDFS_TMEP = "tmp";
		
		public static long SYSTEM_CHECK_PERIOD = 1000 * 20; //non-configurable
		
		public static long HEART_BEAT_FREQ = 1000 * 2;
		
		public static long INCONSISTENCY_LATENCY = 3 * HEART_BEAT_FREQ;  //6s
		
	}
	
	public static class DataNode {
		
		public static int DATA_NODE_REGISTRY_PORT;
		
		public static int DATA_NODE_SERVER_PORT = 6002;  //TODO: CHANGE TO CONFIGURABLE
	}

}
