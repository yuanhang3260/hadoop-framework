package mapreduce.jobtracker;

/**
 * Define the status of job / task 
 *
 */
public enum WorkStatus {
	READY, RUNNING, FAILED, TERMINATED, SUCCESS;
}
