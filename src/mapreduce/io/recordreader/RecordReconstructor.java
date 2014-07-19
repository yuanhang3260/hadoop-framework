package mapreduce.io.recordreader;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import mapreduce.io.KeyValue;
import mapreduce.io.KeyValueCollection;
import mapreduce.io.writable.Writable;

public class RecordReconstructor<K extends Writable, V extends Writable> {
	
	List<KeyValue<K, V>> list = new ArrayList<KeyValue<K, V>>();
	List<KeyValueCollection<K, V>> finalList= new ArrayList<KeyValueCollection<K, V>>();
	Object syncMutex = new Object();
	
	int index = 0;
	
	public void reconstruct(String filename) throws FileNotFoundException, IOException, ClassNotFoundException {
		System.err.println("RecordRecontsructor.reconstruct(): Finish collecet file=" + filename);
		ObjectInputStream in = new ObjectInputStream(new FileInputStream(new File(filename)));
		
		try {
			while (true) {
				KeyValue<K, V> tmp = (KeyValue<K,V>)in.readObject();
				synchronized (this.syncMutex) {
					list.add(tmp);
				}
			}
		} catch (EOFException e) {
			in.close();
		}
		

	}
	
	public void sort() {
		Collections.sort(list);
	}
	
	public void printList() {
		int i  = 1;
		for (KeyValue<K, V> pair : this.list) {
			System.out.format("%d\tkey:%s\tvalue:%s\n", i, pair.getKey().toString(), pair.getValue().getHashValue());
			i++;
		}
	}
	
	public void printFinalList() {
		int i = 1;
		for (KeyValueCollection<K, V> collection : this.finalList) {
			System.out.format("%d\tkey:%s\tvalue:", i, collection.getKey().toString());
			Iterator<V> it = collection.getValues();
			while(it.hasNext()) {
				System.out.format("%s\t", it.next().toString());
				i++;
			}
			System.out.println();
		}
	}
	
	public void addKeyValue(KeyValue<K, V> pair) {
		this.list.add(pair);
	}


	public KeyValueCollection<K, V> nextKeyValueCollection() {
		return this.finalList.get(index++);
	}

	public boolean hasNext() {
		return (index < this.finalList.size());
	}
	
	public void merge() {
		List<List<V>> valueLists = new ArrayList<List<V>>();
		List<K> keyList = new ArrayList<K>();
		List<V> valueList = new ArrayList<V>();
		
		for (KeyValue<K, V> pair : this.list) {
			if (keyList.size() == 0) {
				keyList.add(pair.getKey());
				valueList.add(pair.getValue());
				continue;
			}
			if (pair.getKey().getHashValue() != keyList.get(keyList.size() - 1).getHashValue()) {
				valueLists.add(valueList);
				valueList = new ArrayList<V>();
				keyList.add(pair.getKey());
			}
			valueList.add(pair.getValue());
		}
		valueLists.add(valueList);
		
		for (int i = 0; i < keyList.size(); i++) {
			K key = keyList.get(i);
			Iterator<V> valueIt = valueLists.get(i).iterator();
			this.finalList.add(new KeyValueCollection<K, V>(key, valueIt));
		}
		return;
	}

}
