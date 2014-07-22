package mapreduce.io.collector;

import global.Hdfs;
import global.MapReduce;
import hdfs.io.HDFSBufferedOutputStream;
import hdfs.io.HDFSFile;
import hdfs.io.HDFSOutputStream;
import hdfs.namenode.NameNodeRemoteInterface;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.Queue;

import mapreduce.io.KeyValue;
import mapreduce.io.Partitioner;
import mapreduce.io.writable.IntWritable;
import mapreduce.io.writable.Text;
import mapreduce.io.writable.Writable;
import mapreduce.message.MapperTask;
import mapreduce.message.Task;

public class OutputCollector<K extends Writable, V extends Writable> {
	
	private Queue<KeyValue<K, V>> keyvalueQueue;
	File[] fileArr;
	FileOutputStream[] fileOutputStreamArr;
	BufferedOutputStream[] bufferedOutputStreamArr;
	Task task;
	int partitionNum;
	
	private int fileCounter = 0;
	private final int FILE_MAX_LINES = MapReduce.Core.FILE_MAX_LINES;
	
	
	/**
	 * OutputCollector constructor: This is for mappers, which
	 * write the outputs to different partition files on local
	 * file system.
	 * @param num
	 */
	public OutputCollector(MapperTask task) {
		this.task = task;
		this.partitionNum = ((MapperTask)this.task).getPartitionNum();
		
		this.keyvalueQueue = new PriorityQueue<KeyValue<K, V>>();
		this.bufferedOutputStreamArr = new BufferedOutputStream[this.partitionNum];
	}
	
	/**
	 * OutputCollector constructor: This is for reducers, which
	 * merges same partitions collected from different mappers
	 * to a file on HDFS.
	 */
	public OutputCollector() {
		this.keyvalueQueue = new PriorityQueue<KeyValue<K, V>>();
		this.bufferedOutputStreamArr = new BufferedOutputStream[1];
	}
	
//	public void writeToLocal() throws IOException {
//		Partitioner<K> partitioner = new Partitioner<K>();
//		OutputCollectorIterator<K, V> it = this.iterator(); 
//		for (int i = 0 ; i < this.partitionNum; i++) {
//			String tmpFileName = task.localFileNameWrapper(i);
//			File tmpFile = new File(tmpFileName);
//			FileOutputStream tmpFOS = new FileOutputStream(tmpFile);
//			this.bufferedOutputStreamArr[i] = new BufferedOutputStream(tmpFOS);
//		}
//
//		while (it.hasNext()) {
//			KeyValue<K, V> keyvalue = it.next();
//			int parNum = partitioner.getPartition(keyvalue.getKey(), this.partitionNum);
//			this.bufferedOutputStreamArr[parNum].write(String.format("%s\t%s\n", 
//					keyvalue.getKey().toString(), keyvalue.getValue().toString())
//					.getBytes());
//		}
//		
//		for (int j = 0 ; j < this.partitionNum; j++) {
//			this.bufferedOutputStreamArr[j].flush();
//			this.bufferedOutputStreamArr[j].close();
//		}
//		
//		return;
//		
//	}
	
	public void writeToHDFS(String filename) throws NotBoundException, IOException {
		Registry nameNodeR = LocateRegistry.getRegistry(Hdfs.Core.NAME_NODE_IP, Hdfs.Core.NAME_NODE_REGISTRY_PORT);
		System.out.println("dst ip:" + Hdfs.Core.NAME_NODE_IP + "\t dst port:" + Hdfs.Core.NAME_NODE_REGISTRY_PORT);
		System.err.println("dst ip:" + Hdfs.Core.NAME_NODE_IP + "\t dst port:" + Hdfs.Core.NAME_NODE_REGISTRY_PORT);
		NameNodeRemoteInterface nameNodeS = (NameNodeRemoteInterface) nameNodeR.lookup(Hdfs.Core.NAME_NODE_SERVICE_NAME);
		HDFSFile file = nameNodeS.create(filename);
		HDFSOutputStream out = file.getOutputStream();
		HDFSBufferedOutputStream bout = new HDFSBufferedOutputStream(out);
		
		for (KeyValue<K, V> pair : this.keyvalueQueue) {
			byte[] content = String.format("%s\t%s\n", pair.getKey().toString(), pair.getValue().toString()).getBytes();
			bout.write(content);
		}
		bout.close();
	}
	
	public void collect(K key, V value) throws IOException {
		
		this.keyvalueQueue.offer(new KeyValue<K, V>(key, value));	

		if ((this.keyvalueQueue.size()) % this.FILE_MAX_LINES == 0) { //write to temp file
			flush();
		}
 	}
	
	public void flush() throws IOException {
		String filePrefix = task.getFilePrefix();
		String filename = String.format("%s/%s-%s-tmp-%d.tmp", 
				filePrefix, this.task.getJobId(), this.task.getTaskId(), this.fileCounter++);
		
		File tmpFile = new File(filename);
		BufferedOutputStream bout = new BufferedOutputStream(new FileOutputStream(tmpFile));
		
		while (!this.keyvalueQueue.isEmpty()) {
			KeyValue<K, V> pair = this.keyvalueQueue.poll();
			String pairString = String.format("%s\t%s\n", pair.getKey().toString(), pair.getValue().toString());
			bout.write(pairString.getBytes());
		}
		bout.flush();
		bout.close();
		this.keyvalueQueue = new PriorityQueue<KeyValue<K, V>>();
	}
	
	public OutputCollectorIterator<K, V> iterator() {
		return new OutputCollectorIterator<K, V>(this.keyvalueQueue.iterator());
	}
	
	public void sort() throws IOException {
		
		Partitioner<Text> partitioner = new Partitioner<Text>();
		
		BufferedReader[] inputArray = new BufferedReader[this.fileCounter];
		BufferedOutputStream[] outputArray = new BufferedOutputStream[this.partitionNum];
		
		/* Initialization */
		for (int i = 0; i < this.fileCounter; i++) {
			String filename = String.format("%s/%s-%s-tmp-%d.tmp",
					task.getFilePrefix(), task.getJobId(), task.getTaskId(), i);
			inputArray[i] = new BufferedReader(new FileReader(filename));
		}
		
		for (int i = 0; i < this.partitionNum; i++) {
			String filename = String.format("%s/%s-%s-%d",
					task.getFilePrefix(), task.getJobId(), task.getTaskId(), i);
			outputArray[i] = new BufferedOutputStream(new FileOutputStream(filename));
		}
		
		Queue<FileKeyValue> heap = new PriorityQueue<FileKeyValue>();
		
		/* External sort */
		for (int i = 0; i < this.fileCounter; i++) {
			String line = inputArray[i].readLine();
			String[] comp = line.split("\t");
			heap.offer(new FileKeyValue(i, new Text(comp[0]), new IntWritable(comp[1])));
		}
		
		while (!heap.isEmpty()) {
			
			FileKeyValue towrite = heap.poll();
			
			int parNum = partitioner.getPartition(towrite.getPair().getKey(), this.partitionNum);
			
			String buff = String.format("%s\t%s\n", towrite.getPair().getKey().toString(), towrite.getPair().getValue().toString());
			
			outputArray[parNum].write(buff.getBytes());
			
			String line = inputArray[towrite.getFileSEQ()].readLine();
			if (line != null) {
				String[] comp = line.split("\t");
				heap.offer(new FileKeyValue(towrite.getFileSEQ(), new Text(comp[0]), new IntWritable(comp[1])));
			}
		}
		
		/* Close */
		for (int i = 0; i < this.fileCounter; i++) {
			inputArray[i].close();;
		}
		
		for (int i = 0; i < this.partitionNum; i++) {
			outputArray[i].flush();
			outputArray[i].close();
		}
		
	}
	
	public void printOutputCollector() {
		int i = 0;
		for (KeyValue<K, V> kv : this.keyvalueQueue) {
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
	
	private class FileKeyValue implements Comparable<FileKeyValue> {
		
		private int fileSEQ;
		private KeyValue<Text, IntWritable> pair;
		
		public FileKeyValue (int seq, Text key, IntWritable value) {
			this.fileSEQ = seq;
			this.pair = new KeyValue(key, value);
		}
		
		public int getFileSEQ() {
			return this.fileSEQ;
		}
		
		public KeyValue<Text, IntWritable> getPair() {
			return this.pair;
		}

		@Override
		public int compareTo(FileKeyValue obj) {
			
			return this.getPair().compareTo(obj.getPair());
			
		}
		
	}
	
}
