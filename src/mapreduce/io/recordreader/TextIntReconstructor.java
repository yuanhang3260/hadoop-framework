package mapreduce.io.recordreader;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;

import mapreduce.io.KeyValue;
import mapreduce.io.KeyValueCollection;
import mapreduce.io.writable.IntWritable;
import mapreduce.io.writable.Text;
import mapreduce.message.PartitionEntry;
import mapreduce.message.ReducerTask;

public class TextIntReconstructor {
	
	private String reducer_tmp_filename;
	private BufferedReader in;
	private KeyValueCollection<Text, IntWritable> nextFeed;
	
	int index = 0;
	
	public void sort(ReducerTask task) throws IOException {
		
		BufferedReader[] inputArray = new BufferedReader[task.getEntries().length];
		BufferedOutputStream out = null;
		
		/* Initialization */
		for (int i = 0; i < task.getEntries().length; i++) {
			PartitionEntry entry = task.getEntries()[i];
			String filename = task.localReducerFileNameWrapper(entry.getTID());
			inputArray[i] = new BufferedReader(new FileReader(filename));
		}
		
		String tmpfilename = String.format("%s/%s-%s-reduce.tmp", 
				task.getFilePrefix(), task.getJobId(), task.getTaskId());
		out = new BufferedOutputStream(new FileOutputStream(tmpfilename));
		this.reducer_tmp_filename = tmpfilename;
		
		
		Queue<FileKeyValue> heap = new PriorityQueue<FileKeyValue>();  //*
		
		/* External sort */
		for (int i = 0; i < task.getEntries().length; i++) {  //TODO: check null pointer
			String line = inputArray[i].readLine();
			if (line != null) {
				String[] comp = line.split("\t");
				heap.offer(new FileKeyValue(i, new Text(comp[0]), new IntWritable(comp[1])));
			}
		}
		
		while (!heap.isEmpty()) {
			
			FileKeyValue towrite = heap.poll();
			
			String buff = String.format("%s\t%s\n", towrite.getPair().getKey().toString(), towrite.getPair().getValue().toString());
			
			out.write(buff.getBytes());
			
			String line = inputArray[towrite.getFileSEQ()].readLine();
			if (line != null) {
				String[] comp = line.split("\t");
				heap.offer(new FileKeyValue(towrite.getFileSEQ(), new Text(comp[0]), new IntWritable(comp[1])));
			}
		}
		
		/* Close */
		for (int i = 0; i < task.getEntries().length; i++) {
			inputArray[i].close();
		}
		
		out.close();
		
		/* delete temp file */
		for (int i = 0; i < task.getEntries().length; i++) {
			PartitionEntry entry = task.getEntries()[i];
			String filename = task.localReducerFileNameWrapper(entry.getTID());
			File tmpFile = new File(filename);
			tmpFile.delete();
		}	
		
		this.in = new BufferedReader(new FileReader(this.reducer_tmp_filename));
		
		String line = null;
		
		boolean hasAtLeastOneLine = ((line = in.readLine()) != null);
		
		if (hasAtLeastOneLine) {
			String[] comp = line.split("\t");
			List<IntWritable> valueList = new ArrayList<IntWritable>();
			valueList.add(new IntWritable(comp[1]));
			this.nextFeed = new KeyValueCollection<Text, IntWritable>(new Text(comp[0]), valueList);
		} else {
			this.nextFeed = null;
		}
	}
	
//	public void printList() {
//		int i  = 1;
//		for (KeyValue<Text, IntWritable> pair : this.list) {
//			System.out.format("%d\tkey:%s\tvalue:%s\n", i, pair.getKey().toString(), pair.getValue().getHashValue());
//			i++;
//		}
//	}
//	
//	public void printFinalList() {
//		int i = 1;
//		for (KeyValueCollection<Text, IntWritable> collection : this.finalList) {
//			System.out.format("%d\tkey:%s\tvalue:", i, collection.getKey().toString());
//			Iterator<IntWritable> it = collection.getValues();
//			while(it.hasNext()) {
//				System.out.format("%s\t", it.next().toString());
//				i++;
//			}
//			System.out.println();
//		}
//	}
	


	public KeyValueCollection<Text, IntWritable> nextKeyValueCollection() throws IOException {
		
		KeyValueCollection<Text, IntWritable> rst = null;
		
		while(true) {
			
			String line = in.readLine();
			
			if (line == null) {
				rst = this.nextFeed;
				this.nextFeed = null;
				break;
			}
			
			String[] comp = line.split("\t");
			
			Text currKey = new Text(comp[0]);
			
			if (comp[0].equals(this.nextFeed.getKey().toString())) { //merge to the list
				this.nextFeed.getValues().add(new IntWritable(comp[1]));
			} else {
				rst = this.nextFeed;
				
				List<IntWritable> valueList = new ArrayList<IntWritable>();
				valueList.add(new IntWritable(comp[1]));
				this.nextFeed = new KeyValueCollection<Text, IntWritable>(new Text(comp[0]), valueList);
				break;
			}
		}
		
		return rst;
		
	}

	public boolean hasNext() throws IOException {

		return this.nextFeed != null;
		
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
