package mapreduce.core;

import java.util.ArrayList;
import java.util.List;

public class OutputCollector<K, V> {

	List<K> keyList;
	List<V> valueList;
	int partitionNum;
	
	public OutputCollector(int num) {
		this.keyList = new ArrayList<K>();
		this.valueList = new ArrayList<V>();
		this.partitionNum = num;
	}
	
	public void writeToLocal(Partitioner partioner) {
		int currentPartition = Integer.MAX_VALUE;
		while (this.hasNext()) {
			K key = this.nextKey();
			V value = this.nextValue();
			if (currentPartition == partioner.getPartition(key, value, this.partitionNum)) {
				//WRITE TO PREVIOUS FILE
			} else {
				//WRITE TO NEW FILE
			}
		}
		
	}
	
	public void collect(K key, V value) {
		this.keyList.add(key);
		this.valueList.add(value);
	}
	
	private boolean hasNext() {
		return false;
	}
	
	private K nextKey() {
		return null;
	}
	
	private V nextValue() {
		return null;
	}
}
