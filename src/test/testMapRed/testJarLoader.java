package test.testMapRed;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Enumeration;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import mapreduce.core.Mapper;
import mapreduce.io.writable.Writable;

public class testJarLoader {
	
	public static void main (String[] args) throws IOException, ClassNotFoundException, IllegalArgumentException, SecurityException, InstantiationException, IllegalAccessException, InvocationTargetException {
		
		String jarFilePath = "WordCount.jar";
		
		JarFile jarFile = new JarFile(jarFilePath);
		
		Enumeration<JarEntry> e = jarFile.entries();
		
		URL[] urls = { new URL("jar:file:" + jarFilePath +"!/") };
		ClassLoader cl = URLClassLoader.newInstance(urls);
		
		Class executeClass = null;
		
		while (e.hasMoreElements()) {
            
			JarEntry je = e.nextElement();
            
			if(je.isDirectory() || !je.getName().endsWith(".class")){
                continue;
            }
            
            String className = je.getName().substring(0,je.getName().length()-6);
            className = className.replace('/', '.');
            if (className.equals("Execute")) {
            	executeClass = cl.loadClass(className);
            }
        }
		
		TempRunner runner = (TempRunner) executeClass.getConstructors()[0].newInstance();
		runner.run();
	}
	
}
