package mapreduce.io;

import java.util.Iterator;

import mapreduce.io.writable.Writable;

public class KeyValueCollection<K extends Writable, V extends Writable> {
	private K key;
	private Iterator<V> valueIterator;
	
	public KeyValueCollection(K key, Iterator<V> valueI) {
		this.key = key;
		this.valueIterator = valueI;
	}
	
	public K getKey() {
		return this.key;
	}
	
	public Iterator<V> getValues() {
		return this.valueIterator;
	}
	
}
