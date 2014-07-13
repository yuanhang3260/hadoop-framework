package mapreduce.io;

public class Text extends Writable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 9216926564051053141L;
	private String value;
	
	public Text(String text) {
		this.value = text;	//Initiate value
		/* Compute its hash value */
		long hashValue = 0;
		for (int i = 0; i < this.value.length(); i++) {
			hashValue+= this.value.charAt(i);
		}
		this.hashValue = hashValue;
	}
	
	public String getText() {
		return this.value;
	}
	
}
