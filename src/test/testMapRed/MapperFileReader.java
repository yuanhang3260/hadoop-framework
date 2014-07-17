package test.testMapRed;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

import mapreduce.io.KeyValue;
import mapreduce.io.writable.IntWritable;
import mapreduce.io.writable.Writable;

public class MapperFileReader {
	public static void main(String[] args) throws IOException, ClassNotFoundException {
		String jid = args[0];
		String tid = args[1];
		int totalPartitions = Integer.parseInt(args[2]);
		
		for (int i = 0; i < totalPartitions; i++) {
			String fileName = String.format("%s-%s-%d", jid, tid, i);
			System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>" + fileName);
			File file = new File(fileName);
			FileInputStream fin = new FileInputStream(file);
			ObjectInputStream in = new ObjectInputStream(fin);
			
			KeyValue<Writable, IntWritable> pair;
			int j = 1;
			try {
	 			while ((pair = (KeyValue<Writable, IntWritable>) in.readObject()) != null) {
					System.out.format("%d\t%s\t%s\n", j++, pair.getKey().toString(), pair.getValue().toString());
				}
			} catch (IOException e) {
				
			}
			in.close();
			fin.close();
			System.out.println("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n\n");
		}
	}
}
