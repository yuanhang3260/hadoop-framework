package mapreduce.io.recordreader;

import hdfs.io.HDFSInputStream;

import java.io.IOException;

import mapreduce.io.KeyValue;
import mapreduce.io.Split;
import mapreduce.io.writable.Text;

public class KeyValueLineRecordReader extends RecordReader<Text, Text>{
	private Split split;
	private String[] values;
	private int index;
	
	public KeyValueLineRecordReader(Split s) {
		this.split = s;
		this.index = 0;
	}
	
	public void parseRecords () throws IOException {
		HDFSInputStream in = this.split.file.getInputStream();
		String content = in.readChunk(this.split.chunkIdx);
		this.values = content.split("\n");
	}
	
	
	
	public KeyValue<Text, Text> nextKeyValue() {
		KeyValue<Text, Text> rst = new KeyValue<Text, Text>(new Text(this.split.file.getName()),new Text(this.values[index]));
		index++;
		return rst;
	}
	
	public boolean hasNext() {
		return (this.index < this.values.length);
	}
}
