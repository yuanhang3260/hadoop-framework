package mapreduce.core;

import mapreduce.io.Writable;

public class RecordReader<K, V> {
	
	public Split split;
	
	public RecordReader(Split s) {
		this.split = s;
	}
	
	public Writable nextKey() {
		return null;
	}
	
	public Writable nextValue() {
		return null;
	}
	
	public boolean hasNext() {
		return false;
	}
	
	private class KeyValue<KEY, VALUE> {
		public KEY key;
		public VALUE value;
		
	}
}
