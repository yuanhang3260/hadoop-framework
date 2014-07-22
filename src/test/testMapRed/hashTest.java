package test.testMapRed;

import mapreduce.io.Partitioner;
import mapreduce.io.writable.IntWritable;

public class hashTest {
	public static void main(String[] args) {
		IntWritable iw = new IntWritable(2);
		//Partitioner<IntWritable, IntWritable> p = new Partitioner<IntWritable, IntWritable>();
		//System.out.println("Partition No.:" + p.getPartition(iw, iw, 3));
	}
}
