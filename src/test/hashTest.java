package test;

public class hashTest {
	public static void main(String[] args) {
		String str = "Geng";
		System.out.format("Init: %d\n", str.hashCode());
		String st2 = "Geng";
		System.out.format("Modified: %d\n", st2.hashCode());
	}
}
