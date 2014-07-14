package mapreduce.core;

import hdfs.IO.HDFSInputStream;
import mapreduce.io.KeyValue;
import mapreduce.io.Text;

public class RecordReader {
	
	private Split split;
	private String[] values;
	private int index;
	
	public RecordReader(Split s) {
		this.split = s;
		HDFSInputStream in = this.split.file.getInputStream();
		String content = in.readChunk(this.split.chunkIdx);
		this.values = content.split("\n");
		this.index = 0;
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
