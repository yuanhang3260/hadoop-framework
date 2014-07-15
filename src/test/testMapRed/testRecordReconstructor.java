package test.testMapRed;

import mapreduce.io.KeyValue;
import mapreduce.io.recordreader.RecordReconstructor;
import mapreduce.io.writable.IntWritable;
import mapreduce.io.writable.Text;

public class testRecordReconstructor {
	
	public static void main(String[] args) {
		
		RecordReconstructor<Text, IntWritable> rr = new RecordReconstructor<Text, IntWritable>();
		KeyValue<Text, IntWritable> data1 = new KeyValue<Text, IntWritable>(new Text("B"), new IntWritable(1));
		KeyValue<Text, IntWritable> data2 = new KeyValue<Text, IntWritable>(new Text("A"), new IntWritable(1));
		KeyValue<Text, IntWritable> data5 = new KeyValue<Text, IntWritable>(new Text("B"), new IntWritable(1));
		KeyValue<Text, IntWritable> data4 = new KeyValue<Text, IntWritable>(new Text("BC"), new IntWritable(1));
		KeyValue<Text, IntWritable> data3 = new KeyValue<Text, IntWritable>(new Text("CD"), new IntWritable(1));
		KeyValue<Text, IntWritable> data6 = new KeyValue<Text, IntWritable>(new Text("C"), new IntWritable(1));
		KeyValue<Text, IntWritable> data7 = new KeyValue<Text, IntWritable>(new Text("CD"), new IntWritable(1));
		rr.addKeyValue(data1);
		rr.addKeyValue(data2);
		rr.addKeyValue(data3);
		rr.addKeyValue(data4);
		rr.addKeyValue(data5);
		rr.addKeyValue(data6);
		rr.addKeyValue(data7);
		rr.sort();
		rr.merge();
		rr.printList();
		System.out.println("-----------");
		rr.printFinalList();
		
		
	}
}
