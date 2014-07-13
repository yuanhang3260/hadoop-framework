package mapreduce.io;

public class LongWritable extends Writable{

	/**
	 * 
	 */
	private static final long serialVersionUID = -4469350421720870388L;	
	private Long value;
	
	public LongWritable(long val) {
		this.value = new Long(val); //Initiate value
	}
	
	public long getValue() {
		return this.value.longValue();
	}

	@Override
	public int getHashValue() {
		return this.value.hashCode();
	}
	
	
}
