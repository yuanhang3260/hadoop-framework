package test.testMapRed;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SyncExample {

    List<String> strList = new ArrayList<String>();
    List<String> synStrList = Collections.synchronizedList(strList);

    public static final void main(String[] args) throws InterruptedException {
        
        SyncExample se = new SyncExample();
        Thread second = se.init();
//        System.err.println("Main:\tTry to obtain the mutex");
//        synchronized(se.synStrList) {
//        	System.err.println("Main:\tObtained the mutex");
//        	se.synStrList.add("MAIN-ONE");
//        	Thread.sleep(10);
//        	se.synStrList.add("MAIN-TWO");
//        	Thread.sleep(10);
//        	se.synStrList.add("MAIN-THREE");
//        	Thread.sleep(10);
//        	se.synStrList.add("MAIN-FOUR");
//        	Thread.sleep(10);
        	
        	se.strList.add("MAIN-ONE");
        	Thread.sleep(10);
        	se.strList.add("MAIN-TWO");
        	Thread.sleep(10);
        	se.strList.add("MAIN-THREE");
        	Thread.sleep(10);
        	se.strList.add("MAIN-FOUR");
        	Thread.sleep(10);
//        }
//        System.err.println("Main:\tRelease the mutex");
        
        second.join();
        se.printList();
    }
    
    public Thread init() {
    	MyRunnable mr = new MyRunnable();
        Thread second = new Thread(mr);
        second.start();
        return second;
    }
    

    public class MyRunnable implements Runnable {
    	
        public void run() {
//        	System.err.println("MyRunnable:\tTry to obtain the mutex.");
//            synchronized(SyncExample.this.synStrList) {
//            	System.err.println("MyRunnable:\tObtained the mutex.");
            	
            	try {
//            		SyncExample.this.synStrList.add("MyRunnable-ONE");
//					Thread.sleep(10);
//					SyncExample.this.synStrList.add("MyRunnable-TWO");
//	            	Thread.sleep(10);
//	            	SyncExample.this.synStrList.add("MyRunnable-THREE");
//	            	Thread.sleep(10);
//	            	SyncExample.this.synStrList.add("MyRunnable-FOUR");
//	            	Thread.sleep(10);

            		SyncExample.this.strList.add("MyRunnable-ONE");
					Thread.sleep(10);
					SyncExample.this.strList.add("MyRunnable-TWO");
	            	Thread.sleep(10);
	            	SyncExample.this.strList.add("MyRunnable-THREE");
	            	Thread.sleep(10);
	            	SyncExample.this.strList.add("MyRunnable-FOUR");
	            	Thread.sleep(10);
            		
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
            	
            	
//            }
//            System.err.println("MyRunnable:\tReleasethe mutex.");
        }
    }
    
    public void printList() {
    	synchronized (this.synStrList) {
    		for (String str : this.synStrList) {
    			System.err.println(str);
    		}
    	}
    }
}
