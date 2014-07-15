package mapreduce.io.recordreader;

import mapreduce.io.KeyValue;
import mapreduce.io.writable.Writable;


public abstract class RecordReader<K extends Writable, V extends Writable> {
	
	public abstract KeyValue<K, V> nextKeyValue();
	
	public abstract boolean hasNext();


}
