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
		
		String fileName = args[0];
		System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>" + fileName);
		File file = new File(fileName);
		FileInputStream fin = new FileInputStream(file);
		ObjectInputStream in = new ObjectInputStream(fin);
		
		KeyValue<Writable, IntWritable> pair;
		int m = 1;
		try {
 			while ((pair = (KeyValue<Writable, IntWritable>) in.readObject()) != null) {
				System.out.format("%d\t%s\t%s\n", m++, pair.getKey().toString(), pair.getValue().toString());
			}
		} catch (IOException e) {
			
		}
		in.close();
		fin.close();
		System.out.println("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n\n");
	}
}
