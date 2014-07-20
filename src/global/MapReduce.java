package global;

/* Registry port range: 1201 - 1250, Server port range: 8000 - 8050*/
public class MapReduce {
	
	public static class Core {
		public static boolean DEBUG = true; //non-configurable
		
		public static String JOB_TRACKER_IP;
		public static int JOB_TRACKER_REGISTRY_PORT;
		
		public static String JOB_TRACKER_SERVICE_NAME = "JobTracker";  //non-configurable
	}
	
	public static class JobTracker {
		public static int MAX_RESCHEDULE_ATTEMPTS ;
		public static long TASK_TRACKER_EXPIRATION;
	}
	
	public static class TaskTracker {	
		
		public static class Common {
			
			public static int HEART_BEAT_FREQ; //milliseconds
			public static String TASK_TRACKER_SERVICE_NAME = "TaskTracker";  //non-configurable
			public static String TEMP_FILE_DIR;
			public static boolean MAPPER_FAULT_TEST = false;  //non-configurable
			public static boolean REDUCER_FAULT_TEST = false;  //non-configurable
			public static int REDUCER_FAILURE_TIMES = 1;  //non-configurable
		}
		
		public static class Individual {
			public static int TASK_TRACKER_REGISTRY_PORT;
			public static int TASK_TRACKER_SERVER_PORT;
			public static int CORE_NUM;
		}
	}
}
