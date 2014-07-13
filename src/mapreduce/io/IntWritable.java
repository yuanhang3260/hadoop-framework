package mapreduce.io;

public class IntWritable extends Writable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1014971413849096367L;
	private int value;
	
	public IntWritable(int val) {
		this.value = val; //Initiate value
		this.hashValue = val; //Initiate hash value
	}
	
	public int getValue() {
		return this.value;
	}
	
	

}
