package mapreduce.core;

import java.util.Iterator;

import mapreduce.io.collector.OutputCollector;
import mapreduce.io.writable.IntWritable;
import mapreduce.io.writable.Text;
import mapreduce.io.writable.Writable;

public abstract class Reducer<K1 extends Writable, V1 extends Writable, K2 extends Writable, V2 extends Writable> {
	
	public abstract void reduce(Text key, Iterator<IntWritable> values, OutputCollector<K2, V2> output);
}
