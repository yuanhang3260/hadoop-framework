package mapreduce.core;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;

public class OutputCollector<K, V> {

	List<K> keyList;
	List<V> valueList;
	int partitionNum;
	String jid;
	String tid;
	
	public OutputCollector(int num) {
		this.keyList = new ArrayList<K>();
		this.valueList = new ArrayList<V>();
		this.partitionNum = num;
	}
	
	public void writeToLocal() throws IOException {
		Partitioner<K, V> partitioner = new Partitioner<K, V>();
		OutputCollectorIterator it = this.iterator(); 
		int currentPartition = Integer.MAX_VALUE;
		File intermediatePartitionFile = null;
		FileOutputStream outFile = null;
		ObjectOutputStream out = null;


		while (it.hasNext()) {
			K key = it.nextKey();
			V value = it.nextValue();
			if (currentPartition == partitioner.getPartition(key, value, this.partitionNum)) {
				out.writeObject(key);
				out.writeObject(value);
			} else {
				if (intermediatePartitionFile != null && outFile != null) {
					out.close();
					outFile.close();
				}
				intermediatePartitionFile = new File(String.format("%s-%s-%s", jid, tid, currentPartition));
				if (!intermediatePartitionFile.exists()) {
					intermediatePartitionFile.createNewFile();
				}
				outFile = new FileOutputStream(intermediatePartitionFile);
				out = new ObjectOutputStream(outFile);
			}
		}
		
		return;
		
	}
	
	public void collect(K key, V value) {
		this.keyList.add(key);
		this.valueList.add(value);
	}
	
	public OutputCollectorIterator iterator() {
		return new OutputCollectorIterator((K[])this.keyList.toArray(), (V[])this.valueList.toArray());
	}
	

	
	private class OutputCollectorIterator {
		K[] keyArray;
		V[] valueArray;
		int keyIndex;
		int valueIndex;
		
		public OutputCollectorIterator(K[] keyArray, V[] valueArray) {
			this.keyArray = keyArray;
			this.valueArray = valueArray;
			this.keyIndex = 0;
			this.valueIndex = 0;
		}
		
		public boolean hasNext() {
			if (keyIndex < keyArray.length && valueIndex < valueArray.length) {
				return true;
			} else {
				return false;
			}
		}
		
		public K nextKey() {
			K nextkey = this.keyArray[this.keyIndex];
			this.keyIndex++;
			return nextkey;
		}
		
		public V nextValue() {
			V nextvalue = this.valueArray[this.valueIndex];
			this.valueIndex++;
			return nextvalue;
		}
		
		
	}
}
