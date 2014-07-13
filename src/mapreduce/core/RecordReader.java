package mapreduce.core;

import mapreduce.io.Writable;

public class RecordReader {
	
	public Writable nextKey() {
		return null;
	}
	
	public Writable nextValue() {
		return null;
	}
	
	public boolean hasNext() {
		return false;
	}
}
