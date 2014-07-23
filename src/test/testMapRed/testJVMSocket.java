package test.testMapRed;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;

public class testJVMSocket {
	
	public static void main(String[] args) {
		
		testJVMSocket obj = new testJVMSocket();
		
		obj.init();
		
	}
	
	private void init () {
		
		try {
			
			Server sv = new Server();
			
			Thread serverTh = new Thread(sv);
			
			serverTh.start();
			
			Thread.sleep(1000);
			
			Socket soc = new Socket("localhost", 1234);
			
			System.out.println("connected to server thread");
			
			BufferedOutputStream out = new BufferedOutputStream(soc.getOutputStream());
			
			System.out.println("Got output stream");
			
			BufferedReader in = new BufferedReader( new InputStreamReader(soc.getInputStream()));
			
			System.out.println("Got input stream");
			
			out.write("hello\n".getBytes());
			out.flush();
			
			System.out.println("wrote hello");
			
			String rst = in.readLine();
			
			System.out.println("received hello");
			
			System.out.println(rst);
			
			out.close();
			in.close();
			soc.close();
			
			
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
	}
	
	
	private class Server implements Runnable {

		@Override
		public void run() {
			
			try {
				ServerSocket serverSoc = new ServerSocket(1234);
				
				while (true) {
					
					Socket soc = serverSoc.accept();
					
					System.out.println("server: Accept connection");
					
					BufferedReader in = new BufferedReader(
							new InputStreamReader(soc.getInputStream()));
					
					BufferedOutputStream out = new BufferedOutputStream(soc.getOutputStream());
					
					System.out.println("server: wait for request ");
					
					String req = in.readLine();
					
					System.out.println("server: read req:" + req);
					
					out.write((req + "\n").getBytes());
					
					out.flush();
					
					in.close();
					out.close();
					soc.close();
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		
	}
}
