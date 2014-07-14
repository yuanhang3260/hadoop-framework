package mapreduce.io;


public class KeyValue<KEY extends Writable, VALUE extends Writable> implements Comparable<KeyValue<KEY, VALUE>> {
	KEY key;
	VALUE value;
	
	public KeyValue (KEY k, VALUE v) {
		this.key = k;
		this.value = v;
	}
	
	public KEY getKey() {
		return this.key;
	}
	
	public VALUE getValue() {
		return this.value;
	}
	
	@Override
	public int compareTo(KeyValue<KEY, VALUE> o) {
		return this.key.toString().compareTo(o.key.toString());
	}
}
