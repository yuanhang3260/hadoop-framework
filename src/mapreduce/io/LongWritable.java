package mapreduce.io;

public class LongWritable extends Writable{

	/**
	 * 
	 */
	private static final long serialVersionUID = -4469350421720870388L;	
	private long value;
	
	public LongWritable(long val) {
		this.value = val; //Initiate value
		this.hashValue = val; //Initiate hash value
	}
	
	public long getValue() {
		return this.value;
	}
}
