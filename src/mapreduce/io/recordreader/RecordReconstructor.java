package mapreduce.io.recordreader;

import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import mapreduce.io.KeyValue;
import mapreduce.io.writable.Writable;

public class RecordReconstructor<K extends Writable, V extends Writable> extends RecordReader<K, V> {
	
	List<KeyValue<K, V>> list = new ArrayList<KeyValue<K, V>>();
	int index = 0;
	
	public void reconstruct(String filename) throws FileNotFoundException, IOException, ClassNotFoundException {
		
		ObjectInputStream in = new ObjectInputStream(new FileInputStream(filename));
		
		try {
			while (true) {
				KeyValue<K, V> tmp = (KeyValue<K,V>)in.readObject();
				list.add(tmp);
			}
		} catch (EOFException e) {
			in.close();
		}
		for (KeyValue<K, V> pair : list) {
			System.out.println(pair.getKey().toString() + "\t" + pair.getValue().toString());
		}
	}
	
	public void sort() {
		Collections.sort(list);
	}
	
	public void printList() {
		int i  = 0;
		for (KeyValue<K, V> pair : this.list) {
			System.out.format("%d\tkey:%s\tvalue:%s", i, pair.getKey().toString(), pair.getValue().getHashValue());
			i++;
		}
	}

	@Override
	public KeyValue<K, V> nextKeyValue() {
		return this.list.get(index++);
	}

	@Override
	public boolean hasNext() {
		return (index < this.list.size());
	}

}
