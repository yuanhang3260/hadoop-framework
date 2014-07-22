package mapreduce.io.recordreader;

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import mapreduce.io.KeyValue;
import mapreduce.io.KeyValueCollection;
import mapreduce.io.writable.IntWritable;
import mapreduce.io.writable.Text;
import mapreduce.io.writable.Writable;

public class TextIntReconstructor {
	
	List<KeyValue<Text, IntWritable>> list = new ArrayList<KeyValue<Text, IntWritable>>();
	List<KeyValueCollection<Text, IntWritable>> finalList= new ArrayList<KeyValueCollection<Text, IntWritable>>();
	
	int index = 0;
	
	public void reconstruct(String filename) throws FileNotFoundException, IOException, ClassNotFoundException {
		
		BufferedReader in = new BufferedReader(new FileReader(filename));
		
		String line;
		
		try {
			while ((line = in.readLine()) != null) {

				String[] comp = line.split("\t");
				KeyValue<Text, IntWritable> tmp = new KeyValue<Text, IntWritable>(new Text(comp[0]), new IntWritable(comp[1]));
				list.add(tmp);
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
		for (KeyValue<Text, IntWritable> pair : this.list) {
			System.out.format("%d\tkey:%s\tvalue:%s\n", i, pair.getKey().toString(), pair.getValue().getHashValue());
			i++;
		}
	}
	
	public void printFinalList() {
		int i = 1;
		for (KeyValueCollection<Text, IntWritable> collection : this.finalList) {
			System.out.format("%d\tkey:%s\tvalue:", i, collection.getKey().toString());
			Iterator<IntWritable> it = collection.getValues();
			while(it.hasNext()) {
				System.out.format("%s\t", it.next().toString());
				i++;
			}
			System.out.println();
		}
	}
	
	public void addKeyValue(KeyValue<Text, IntWritable> pair) {
		this.list.add(pair);
	}


	public KeyValueCollection<Text, IntWritable> nextKeyValueCollection() {
		return this.finalList.get(index++);
	}

	public boolean hasNext() {
		return (index < this.finalList.size());
	}
	
	public void merge() {
		List<List<IntWritable>> valueLists = new ArrayList<List<IntWritable>>();
		List<Text> keyList = new ArrayList<Text>();
		List<IntWritable> valueList = new ArrayList<IntWritable>();
		
		for (KeyValue<Text, IntWritable> pair : this.list) {
			if (keyList.size() == 0) {
				keyList.add(pair.getKey());
				valueList.add(pair.getValue());
				continue;
			}
			if (pair.getKey().getHashValue() != keyList.get(keyList.size() - 1).getHashValue()) {
				valueLists.add(valueList);
				valueList = new ArrayList<IntWritable>();
				keyList.add(pair.getKey());
			}
			valueList.add(pair.getValue());
		}
		valueLists.add(valueList);
		
		for (int i = 0; i < keyList.size(); i++) {
			Text key = keyList.get(i);
			Iterator<IntWritable> valueIt = valueLists.get(i).iterator();
			this.finalList.add(new KeyValueCollection<Text, IntWritable>(key, valueIt));
		}
		return;
	}

}
