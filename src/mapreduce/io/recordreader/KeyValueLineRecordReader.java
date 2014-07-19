package mapreduce.io.recordreader;

import hdfs.IO.HDFSInputStream;
import mapreduce.io.KeyValue;
import mapreduce.io.Split;
import mapreduce.io.writable.Text;

public class KeyValueLineRecordReader extends RecordReader<Text, Text>{
	private Split split;
	private String[] values;
	private int index;
	
	public KeyValueLineRecordReader(Split s) throws Exception {
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
