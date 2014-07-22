package mapreduce.io;

import mapreduce.io.writable.Writable;

public class Partitioner<KEY extends Writable> {
		
	public int getPartition(KEY key, int partitionNum) {
		return (int)(((long)key.getHashValue() - (long)key.MIN_KEY) / (((long)key.MAX_KEY - (long)key.MIN_KEY) / (long) partitionNum));
	}
}