package mapreduce.core;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import mapreduce.io.KeyValue;
import mapreduce.io.Writable;

public class OutputCollector<K extends Writable, V extends Writable> {
	
	List<KeyValue<K, V>> keyvalueList;
	int partitionNum;
	String jid;
	String tid;
	File[] fileArr;
	FileOutputStream[] fileOutputStreamArr;
	PrintWriter[] printWriterArr;
	
	public OutputCollector(int num) {
		this.keyvalueList = new ArrayList<KeyValue<K, V>>();
		this.partitionNum = num;
		this.printWriterArr = new PrintWriter[num];
	}
	
	public void writeToLocal() throws IOException {
		Partitioner<K, V> partitioner = new Partitioner<K, V>();
		OutputCollectorIterator<K, V> it = this.iterator(); 
		for (int i = 0 ; i < this.partitionNum; i++) {
			String tmpFileName = tmpOutputFileName(this.jid, this.tid, i);
			File tmpFile = new File(tmpFileName);
			FileOutputStream tmpFOS = new FileOutputStream(tmpFile);
			this.printWriterArr[i] = new PrintWriter(tmpFOS);
		}
		
		while (it.hasNext()) {
			KeyValue<K, V> keyvalue = it.next();
			int parNum = partitioner.getPartition(keyvalue.getKey(), keyvalue.getValue(), this.partitionNum);
			this.printWriterArr[parNum].print(keyvalue.getKey().toString());
			this.printWriterArr[parNum].print("\t");
			this.printWriterArr[parNum].println(keyvalue.getValue().toString());
		}
		
		
		for (int j = 0 ; j < this.partitionNum; j++) {
			this.printWriterArr[j].flush();
			this.printWriterArr[j].close();
		}
		
		return;
		
	}
	
	public void collect(K key, V value) {
		this.keyvalueList.add(new KeyValue<K, V>(key, value));
	}
	
	public OutputCollectorIterator<K, V> iterator() {
		return new OutputCollectorIterator<K, V>(this.keyvalueList.iterator());
	}
	
	public void sort() {
		Collections.sort(this.keyvalueList);
	}
	
	public void printOutputCollector() {
		int i = 0;
		for (KeyValue<K, V> kv : this.keyvalueList) {
			i++;
			System.out.format("%d\t%s-%s\n",i,kv.getKey().toString(), kv.getValue().toString());	
		}
	}
	

	
	private class OutputCollectorIterator<KEY1 extends Writable, VALUE1 extends Writable> {
		Iterator<KeyValue<KEY1, VALUE1>> it;
		
		public OutputCollectorIterator(Iterator<KeyValue<KEY1, VALUE1>> it) {
			this.it = it;
		}
		
		public boolean hasNext() {
			return it.hasNext();
		}
		
		public KeyValue<KEY1, VALUE1> next() {
			return it.next();
		}
	}
	
	public static String tmpOutputFileName(String jid, String tid, int partitionNum) {
		return String.format("%s-%s-%d", jid, tid, partitionNum);
	}

	
}
