package global;

/* Registry port range: 1201 - 1250, Server port range: 8000 - 8050*/
public class MapReduce {
	
	public static class Common {
		public static boolean DEBUG = true;
	}
	
	
	public static boolean UNITEST = false;

	
	public static class JobTracker {
		public static String jobTrackerServiceName = "JobTracker";
		public static String jobTrackerRegistryIp = "128.237.213.225";

		public static int MAX_RESCHEDULE_ATTEMPTS = 2;
		public static long TASK_TRACKER_EXPIRATION = 1000 * 40;

		public static int jobTrackerRegistryPort = 2000;
	}
	
	public static class TaskTracker {
		public static String taskTrackerServiceName = "TaskTracker";
		public static int MAX_NUM_MAP_TASK = 9;
		public static int MAX_NUM_REDUCE_TASK = 3;
		public static int HEART_BEAT_FREQ = 1000; //milliseconds
		public static int BUFF_SIZE = 1024 * 1024;
		public static String TEMP_FILE_DIR = "tmp";
		public static boolean MAPPER_FAULT_TEST = false;
		public static boolean REDUCER_FAULT_TEST = false;
		public static int REDUCER_FAILURE_TIMES = 1;
	}
	
	public static class TaskTracker1 {
		public static String taskTrackerIp = "128.237.213.225";
		public static int taskTrackerPort = 1200;
		public static int taskTrackerServerPort = 8001;
		public static int CORE_NUM = 4;
	}
	
	public static class TasktTracker2 {
		public static String taskTrackerIp = "localhost";
		public static int taskTrackerPort = 1201;
		public static int taskTrackerServerPort = 8001;
		public static int CORE_NUM = 4;
	}
	
	public static class TaskTrackerTest1 {
		public static String fileName = "wordCount";
	}
}
