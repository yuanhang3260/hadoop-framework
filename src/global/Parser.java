package global;

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
	
	public static void hdfsCommonConf() throws ParserConfigurationException, 
		SAXException, IOException, ConfFormatException, NumberFormatException {
		
		File fXmlFile = new File("./conf/hdfs-core.xml");
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
				Hdfs.Common.DEBUG = eElement.getElementsByTagName("debug").item(0).getTextContent().trim().equals("true");
				
				/* Parse replica factor */
				Hdfs.Common.REPLICA_FACTOR = 
						Integer.parseInt(eElement.getElementsByTagName("replica-factor")
								.item(0).getTextContent().trim());
				
				
				if (Hdfs.Common.REPLICA_FACTOR <= 0) {
					throw new ConfFormatException(String.format("replica-factor:"
							+ "%d is less than 1", Hdfs.Common.REPLICA_FACTOR));
				}
				
				/* Parse chunk size */
				Hdfs.Common.CHUNK_SIZE =
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
					
				Hdfs.Common.CHUNK_SIZE *= unit;
				
				
				
				/* Parse read buffer size */
				Hdfs.Common.READ_BUFF_SIZE =
						Integer.parseInt(eElement.getElementsByTagName("read-buff-size")
								.item(0).getTextContent().trim());
				
				unitType = 
						((Element)eElement.getElementsByTagName("read-buff-size").item(0)).getAttribute("unit");
				
				unit = parseUnit(unitType);
				
				Hdfs.Common.READ_BUFF_SIZE *= unit;
				
				if (Hdfs.Common.READ_BUFF_SIZE < 1) {
					throw new ConfFormatException ("read buffer size cannot be non-positive");
				}
				
				
				
				/* Parse write buffer size*/
				Hdfs.Common.WRITE_BUFF_SIZE =
						Integer.parseInt(eElement.getElementsByTagName("write-buff-size")
								.item(0).getTextContent().trim());
				
				unitType = 
						((Element)eElement.getElementsByTagName("write-buff-size").item(0)).getAttribute("unit");
				
				unit = parseUnit(unitType);
				
				Hdfs.Common.WRITE_BUFF_SIZE *= unit;
				
				if (Hdfs.Common.WRITE_BUFF_SIZE < 1) {
					throw new ConfFormatException ("write buffer size cannot be non-positive");
				}
				
				
				/* Parse write buffer size*/
				Hdfs.Common.PARTITION_TOLERANCE =
						Integer.parseInt(eElement.getElementsByTagName("partition-tolerance")
								.item(0).getTextContent().trim());
				
				unitType = 
						((Element)eElement.getElementsByTagName("partition-tolerance").item(0)).getAttribute("unit");
				
				unit = parseUnit(unitType);
				
				Hdfs.Common.PARTITION_TOLERANCE *= unit;
				
				if (Hdfs.Common.PARTITION_TOLERANCE < 1) {
					throw new ConfFormatException ("partition tolerance cannot be non-positive");
				}
				
				
				
				

				
			}
		}
	}
	
	public static void nameNodeConf() throws ParserConfigurationException, SAXException, IOException, ConfFormatException {
		
		hdfsCommonConf();
		
		IPAddressValidator ipValidator = new IPAddressValidator();
		
		File fXmlFile = new File("./conf/hdfs-core.xml");
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
		Document doc = dBuilder.parse(fXmlFile);
		doc.getDocumentElement().normalize();
		NodeList nList = doc.getElementsByTagName("namenode");
		 
		for (int i = 0; i < nList.getLength(); i++) {
	 
			Node nNode = nList.item(i);
	 
			if (nNode.getNodeType() == Node.ELEMENT_NODE) {
				
				Element eElement = (Element) nNode;
				
				/* parse NameNode IP Address */
				Hdfs.NameNode.nameNodeRegistryIP =
						eElement.getElementsByTagName("ip")
								.item(0).getTextContent().trim();
				
				if (!ipValidator.validate(Hdfs.NameNode.nameNodeRegistryIP)) {
					throw new ConfFormatException("NameNode IP value is invalid");
				}
				
				
				/* parse NameNode port */
				Hdfs.NameNode.nameNodeRegistryPort =
						Integer.parseInt(eElement.getElementsByTagName("port")
								.item(0).getTextContent().trim());
				
				if (Hdfs.NameNode.nameNodeRegistryPort  < 1024) {
					throw new ConfFormatException ("NameNode port cannot use a well-known port.");
				} else if (Hdfs.NameNode.nameNodeRegistryPort  > 65535){
					throw new ConfFormatException (String.format("NameNode port(%d) cannot be larger than 65535.", Hdfs.NameNode.nameNodeRegistryPort));
				}
				
				
			}
		}

		return;
	}
	
	private static int parseUnit (String unitType) {
		int unit;
		if (unitType.equals("B")) {
			unit = 1;
		} else if (unitType.equals("KB")) {
			unit = 1024;
		} else if (unitType.equals("MB")) {
			unit = 1024 * 1024;
		} else if (unitType.equals("sec")){
			unit = 1000;
		} else if (unitType.equals("min")) {
			unit = 1000 * 50;
		} else {
			unit = 1;
		}
		return unit;
	}
	
	public static void printConf(String[] args) {
		for (String arg : args) {
			if (arg.equals("HDFSCommon")) {
				printHDFSCommonConf();
			} else if (arg.equals("NameNode")){
				printHDFSNameNodeConf();
			}
		}
		
	}
	
	public static void printHDFSCommonConf() {
		System.out.println("HDFS Common:");
		System.out.println("HDFS.Common.DEBUG = " + Hdfs.Common.DEBUG);
		System.out.println("HDFS.Common.REPLICA_FACTOR = " + Hdfs.Common.REPLICA_FACTOR);
		System.out.println("HDFS.Common.CHUNK_SIZE = " + Hdfs.Common.CHUNK_SIZE);
		System.out.println("HDFS.Common.WRITE_BUFF_SIZE = " + Hdfs.Common.WRITE_BUFF_SIZE);
		System.out.println("HDFS.Common.READ_BUFF_SIZE = " + Hdfs.Common.READ_BUFF_SIZE);
		System.out.println("HDFS.Common.PARTITION_TOLERANCE = " + Hdfs.Common.PARTITION_TOLERANCE);
		System.out.println("\n\n\n");
	}
	
	public static void printHDFSNameNodeConf() {
		System.out.println("HDFS NameNode:");
		System.out.println("HDFS NameNode.IP = " + Hdfs.NameNode.nameNodeRegistryIP);
		System.out.println("HDFS NameNode.Port = " + Hdfs.NameNode.nameNodeRegistryPort);
		System.out.println("\n\n\n");
	}
	
	public static class ConfFormatException extends Exception {
		
		public ConfFormatException(String message) {
			super(message);
		}
	}
}