package test;

import mapreduce.core.Partitioner;
import mapreduce.io.IntWritable;

public class hashTest {
	public static void main(String[] args) {
		IntWritable iw = new IntWritable(2);
		Partitioner<IntWritable, IntWritable> p = new Partitioner<IntWritable, IntWritable>();
		System.out.println("Partition No.:" + p.getPartition(iw, iw, 3));
	}
}
