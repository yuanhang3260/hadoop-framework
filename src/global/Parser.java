package global;

import global.Hdfs;

import java.io.File;
import java.io.IOException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class Parser {
	
	public static void nameNodeParseConf() throws ParserConfigurationException, SAXException, IOException {
		

		File fXmlFile = new File("./conf/hdfs.core.xml");
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
		Document doc = dBuilder.parse(fXmlFile);
		doc.getDocumentElement().normalize();
		NodeList nList = doc.getElementsByTagName("core");
		 
		for (int i = 0; i < nList.getLength(); i++) {
	 
			Node nNode = nList.item(i);
	 
			if (nNode.getNodeType() == Node.ELEMENT_NODE) {
	 
				Element eElement = (Element) nNode;
				
				/* Parse DEBUG */
				Hdfs.DEBUG = eElement.getElementsByTagName("debug").item(0).getTextContent().trim().equals("true");
				
				/* Parse replica factor */
				try {
					Hdfs.NameNode.REPLICA_FACTOR = 
							Integer.parseInt(eElement.getElementsByTagName("replica-factor")
									.item(0).getTextContent().trim());
				} catch (NumberFormatException e) {
					Hdfs.NameNode.REPLICA_FACTOR = 2;
				}
				
				/* Parse buffer size */
				try {
					Hdfs.Client.READ_BUFFER_SIZE =
							Integer.parseInt(eElement.getElementsByTagName("buff-size")
									.item(0).getTextContent().trim());
					int unit = 0;
					String unitType = 
							((Element)eElement.getElementsByTagName("buff-size").item(0)).getAttribute("unit");
					if (unitType.equals("B")) {
						unit = 1;
					} else if (unitType.equals("KB")) {
						unit = 1024;
					} else if (unitType.equals("MB")) {
						unit = 1024 * 1024;
					} else {
						unit = 1;
					}
					
					Hdfs.Client.READ_BUFFER_SIZE *= unit;
					if (Hdfs.Client.READ_BUFFER_SIZE <= 0) {
						throw new NonPositiveException("buffer size cannot be non-positive");
					}
				} catch (NumberFormatException e) {
					Hdfs.Client.READ_BUFFER_SIZE = 1024 * 4; //Default buffer size is 4K
				} catch (NonPositiveException e) {
					Hdfs.Client.READ_BUFFER_SIZE = 1024 * 4;
				}
				
				/* Parse chunk size */
				try {
					Hdfs.NameNode.CHUNK_SIZE =
							Integer.parseInt(eElement.getElementsByTagName("chunk-size")
									.item(0).getTextContent().trim());
					int unit = 0;
					String unitType = 
							((Element)eElement.getElementsByTagName("chunk-size").item(0)).getAttribute("unit");
					if (unitType.equals("B")) {
						unit = 1;
					} else if (unitType.equals("KB")) {
						unit = 1024;
					} else if (unitType.equals("MB")) {
						unit = 1024 * 1024;
					} else {
						unit = 1;
					}
					
					Hdfs.NameNode.CHUNK_SIZE *= unit;
				} catch (NumberFormatException e) {
					Hdfs.NameNode.CHUNK_SIZE *= 10;
				}
	 
			}
		}

		return;
	}
	
	private static class NonPositiveException extends Exception {
		
		public NonPositiveException(String message) {
			super(message);
		}
	}
	
}
