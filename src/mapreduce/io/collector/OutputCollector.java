package mapreduce.io.collector;

import global.Hdfs;
import hdfs.DataStructure.HDFSFile;
import hdfs.IO.HDFSBufferedOutputStream;
import hdfs.IO.HDFSOutputStream;
import hdfs.NameNode.NameNodeRemoteInterface;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.rmi.NotBoundException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import mapreduce.io.KeyValue;
import mapreduce.io.Partitioner;
import mapreduce.io.writable.Writable;
import mapreduce.task.MapperTask;
import mapreduce.task.Task;

public class OutputCollector<K extends Writable, V extends Writable> {
	
	List<KeyValue<K, V>> keyvalueList;
	File[] fileArr;
	FileOutputStream[] fileOutputStreamArr;
	ObjectOutputStream[] objOutputStreamArr;
	Task task;
	int partitionNum;
	
	
	/**
	 * OutputCollector constructor: This is for mappers, which
	 * write the outputs to different partition files on local
	 * file system.
	 * @param num
	 */
	public OutputCollector(MapperTask task) {
		this.task = task;
		this.partitionNum = ((MapperTask)this.task).getPartitionNum();
		
		this.keyvalueList = new ArrayList<KeyValue<K, V>>();
		this.objOutputStreamArr = new ObjectOutputStream[this.partitionNum];
	}
	
	/**
	 * OutputCollector constructor: This is for reducers, which
	 * merges same partitions collected from different mappers
	 * to a file on HDFS.
	 */
	public OutputCollector() {
		this.keyvalueList = new ArrayList<KeyValue<K, V>>();
		this.objOutputStreamArr = new ObjectOutputStream[1];
	}
	
	public void writeToLocal() throws IOException {
		Partitioner<K, V> partitioner = new Partitioner<K, V>();
		OutputCollectorIterator<K, V> it = this.iterator(); 
		for (int i = 0 ; i < this.partitionNum; i++) {
			String tmpFileName = task.localFileNameWrapper(i);
			File tmpFile = new File(tmpFileName);
			FileOutputStream tmpFOS = new FileOutputStream(tmpFile);
			this.objOutputStreamArr[i] = new ObjectOutputStream(tmpFOS);
		}
		
		while (it.hasNext()) {
			KeyValue<K, V> keyvalue = it.next();
			int parNum = partitioner.getPartition(keyvalue.getKey(), keyvalue.getValue(), this.partitionNum);
			this.objOutputStreamArr[parNum].writeObject(keyvalue);
		}
		
		
		for (int j = 0 ; j < this.partitionNum; j++) {
			this.objOutputStreamArr[j].flush();
			this.objOutputStreamArr[j].close();
		}
		
		return;
		
	}
	
	public void writeToHDFS(String filename) throws NotBoundException, IOException {
		Registry nameNodeR = LocateRegistry.getRegistry(Hdfs.Core.NAME_NODE_IP, Hdfs.Core.NAME_NODE_REGISTRY_PORT);
		NameNodeRemoteInterface nameNodeS = (NameNodeRemoteInterface) nameNodeR.lookup(Hdfs.Core.NAME_NODE_SERVICE_NAME);
		HDFSFile file = nameNodeS.create(filename);
		HDFSOutputStream out = file.getOutputStream();
		HDFSBufferedOutputStream bout = new HDFSBufferedOutputStream(out);
		
		for (KeyValue<K, V> pair : this.keyvalueList) {
			byte[] content = String.format("%s\t%s\n", pair.getKey().toString(), pair.getValue().toString()).getBytes();
			bout.write(content);
		}
		bout.close();
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
	
}
