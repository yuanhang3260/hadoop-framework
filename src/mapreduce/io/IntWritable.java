package mapreduce.io;

public class IntWritable extends Writable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1014971413849096367L;
	private Integer value;
	
	public IntWritable(int val) {
		this.value = new Integer(val); //Initiate value
	}
	
	public int getValue() {
		return this.value.intValue();
	}

	@Override
	public int getHashValue() {
		return this.value.hashCode();
	}
	
	

}