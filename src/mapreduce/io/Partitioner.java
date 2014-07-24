package mapreduce.io;

import mapreduce.io.writable.Writable;

public class Partitioner<KEY extends Writable> {
		
	public int getPartition(KEY key, int partitionNum) {
		
		return Math.abs((int)((long)key.getHashValue() % ((long) partitionNum)));
		
	}
}