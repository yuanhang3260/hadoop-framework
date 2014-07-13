package mapreduce.core;

public class Partitioner<KEY, VALUE> {
	
	public int getPartition(KEY key, VALUE value, int partitionNum) {
		return 0;
	}
}
