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
	
	public static void main (String[] args) throws NumberFormatException, ParserConfigurationException, SAXException, IOException, ConfFormatException {
		hdfsCoreConf();
		dataNodeConf();
		printConf(new ConfOpt[] {ConfOpt.HDFSCORE, ConfOpt.DATANODE});
		
		mapreduceCoreConf();
		printConf(new ConfOpt[] {ConfOpt.MAPREDCORE});
		
		mapreduceJobTrackerConf();
		printConf(new ConfOpt[] {ConfOpt.JOBTACKER});
		
		mapreduceTaskTrackerCommonConf();
		printConf(new ConfOpt[] {ConfOpt.TASKTRACKERCOMMON});
		
		mapreduceTaskTrackerIndividualConf();
		printConf(new ConfOpt[] {ConfOpt.TASKTRACKERINDIVIDUAL});
		
		
	}
	
	public static void hdfsCoreConf() throws ParserConfigurationException, 
		SAXException, IOException, ConfFormatException, NumberFormatException {
		
		IPAddressValidator ipValidator = new IPAddressValidator();
		
		File fXmlFile = new File("./conf/hdfs.xml");
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
		Document doc = dBuilder.parse(fXmlFile);
		doc.getDocumentElement().normalize();
		NodeList nList = doc.getElementsByTagName("core");
		
		for (int i = 0; i < nList.getLength(); i++) {
			
			Node nNode = nList.item(i);
			
			if (nNode.getNodeType() == Node.ELEMENT_NODE) {
				Element eElement = (Element) nNode;

				/* Parse replica factor */
				Hdfs.Core.REPLICA_FACTOR = 
						Integer.parseInt(eElement.getElementsByTagName("replica-factor")
								.item(0).getTextContent().trim());
				
				
				if (Hdfs.Core.REPLICA_FACTOR < 1) {
					throw new ConfFormatException(String.format("replica-factor:"
							+ "%d is less than 1", Hdfs.Core.REPLICA_FACTOR));
				}
				
				/* Parse chunk size */
				Hdfs.Core.CHUNK_SIZE =
						Integer.parseInt(eElement.getElementsByTagName("chunk-size")
								.item(0).getTextContent().trim());
				
				String unitType = 
						((Element)eElement.getElementsByTagName("chunk-size").item(0)).getAttribute("unit");
				int unit = parseUnit(unitType);
				Hdfs.Core.CHUNK_SIZE *= unit;
				
				/* Parse read buffer size */
				Hdfs.Core.READ_BUFF_SIZE =
						Integer.parseInt(eElement.getElementsByTagName("read-buff-size")
								.item(0).getTextContent().trim());
				
				unitType = 
						((Element)eElement.getElementsByTagName("read-buff-size").item(0)).getAttribute("unit");
				unit = parseUnit(unitType);
				Hdfs.Core.READ_BUFF_SIZE *= unit;
				
				if (Hdfs.Core.READ_BUFF_SIZE < 1) {
					throw new ConfFormatException ("read buffer size cannot be non-positive");
				}
				
				
				/* Parse write buffer size*/
				Hdfs.Core.WRITE_BUFF_SIZE =
						Integer.parseInt(eElement.getElementsByTagName("write-buff-size")
								.item(0).getTextContent().trim());
				
				unitType = 
						((Element)eElement.getElementsByTagName("write-buff-size").item(0)).getAttribute("unit");
				unit = parseUnit(unitType);
				Hdfs.Core.WRITE_BUFF_SIZE *= unit;
				
				if (Hdfs.Core.WRITE_BUFF_SIZE < 1) {
					throw new ConfFormatException ("write buffer size cannot be non-positive");
				}
				
				
				/* Parse partition tolerance */
				Hdfs.Core.PARTITION_TOLERANCE =
						Integer.parseInt(eElement.getElementsByTagName("partition-tolerance")
								.item(0).getTextContent().trim());
				
				unitType = 
						((Element)eElement.getElementsByTagName("partition-tolerance").item(0)).getAttribute("unit");
				unit = parseUnit(unitType);
				Hdfs.Core.PARTITION_TOLERANCE *= unit;
				
				if (Hdfs.Core.PARTITION_TOLERANCE < 1) {
					throw new ConfFormatException ("partition tolerance cannot be non-positive");
				}
				
				
				/* parse NameNode IP Address */
				Hdfs.Core.NAME_NODE_IP =
						eElement.getElementsByTagName("ip")
								.item(0).getTextContent().trim();
				
				if (!ipValidator.validate(Hdfs.Core.NAME_NODE_IP)) {
					throw new ConfFormatException("NameNode IP value is invalid");
				}
				
				/* parse NameNode port */
				Hdfs.Core.NAME_NODE_REGISTRY_PORT =
						Integer.parseInt(eElement.getElementsByTagName("port")
								.item(0).getTextContent().trim());
				
				if (Hdfs.Core.NAME_NODE_REGISTRY_PORT  < 1024) {
					throw new ConfFormatException ("NameNode port cannot use a well-known port.");
				} else if (Hdfs.Core.NAME_NODE_REGISTRY_PORT  > 65535){
					throw new ConfFormatException (String.format("NameNode port(%d) cannot be larger than 65535.", Hdfs.Core.NAME_NODE_REGISTRY_PORT));
				}
				
			}
		}
	}
	
	public static void dataNodeConf() throws NumberFormatException, ParserConfigurationException, SAXException, IOException, ConfFormatException {
		
		hdfsCoreConf();
		
		File fXmlFile = new File("./conf/hdfs.xml");
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
		Document doc = dBuilder.parse(fXmlFile);
		doc.getDocumentElement().normalize();
		
		NodeList nList = doc.getElementsByTagName("datanode");
		
		Node nNode = nList.item(0);
 
		if (nNode.getNodeType() == Node.ELEMENT_NODE) {
			
			Element eElement = (Element) nNode;
			
			/* parse the DataNode port num */
			try {
				Hdfs.DataNode.DATA_NODE_REGISTRY_PORT =
						Integer.parseInt(eElement.getElementsByTagName("registry-port")
								.item(0).getTextContent().trim());
			} catch (NumberFormatException e) {
				throw e;
			}
			
			try {
				Hdfs.DataNode.DATA_NODE_SERVER_PORT =
						Integer.parseInt(eElement.getElementsByTagName("server-port")
								.item(0).getTextContent().trim());
			} catch (NumberFormatException e) {
				throw e;
			}
		}
	}
	
	public static void mapreduceCoreConf() throws ParserConfigurationException, 
	SAXException, IOException, ConfFormatException, NumberFormatException {
		
		File fXmlFile = new File("./conf/mapreduce.xml");
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
		Document doc = dBuilder.parse(fXmlFile);
		doc.getDocumentElement().normalize();
		
		NodeList nList = doc.getElementsByTagName("core");
		
		Node nNode = nList.item(0);
		
		if (nNode.getNodeType() == Node.ELEMENT_NODE) {
			
			Element eElement = (Element) nNode;

			/*-------------- Parse JobTracker IP ----------------*/
			MapReduce.Core.JOB_TRACKER_IP = 
					eElement.getElementsByTagName("jobtracker-ip")
							.item(0).getTextContent().trim();
			
			IPAddressValidator ipValidator = new IPAddressValidator();
			
			if (!ipValidator.validate(MapReduce.Core.JOB_TRACKER_IP)) {
				throw new ConfFormatException("JobTracker IP value is invalid");
			}
			
			
			/*-------------- Parse JobTracker Registry Port ----------------*/
			MapReduce.Core.JOB_TRACKER_REGISTRY_PORT =
					Integer.parseInt(eElement.getElementsByTagName("jobtracker-registry-port")
							.item(0).getTextContent().trim());
			
			if (MapReduce.Core.JOB_TRACKER_REGISTRY_PORT  < 1024) {
				throw new ConfFormatException ("JobTracker port cannot use a well-known port.");
			} else if (MapReduce.Core.JOB_TRACKER_REGISTRY_PORT  > 65535){
				throw new ConfFormatException (String.format("JobTracker port(%d) cannot be larger than 65535."
						, MapReduce.Core.JOB_TRACKER_REGISTRY_PORT));
			}
			
		}	
	}
	
	public static void mapreduceJobTrackerConf() throws ParserConfigurationException, 
	SAXException, IOException, ConfFormatException, NumberFormatException {
		
		File fXmlFile = new File("./conf/mapreduce.xml");
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
		Document doc = dBuilder.parse(fXmlFile);
		doc.getDocumentElement().normalize();
		
		NodeList nList = doc.getElementsByTagName("jobtracker");
		
		Node nNode = nList.item(0);
		
		if (nNode.getNodeType() == Node.ELEMENT_NODE) {
			
			Element eElement = (Element) nNode;
			
			/*-------------- Parse Maximum Re-schedule Attempts ----------------*/
			MapReduce.JobTracker.MAX_RESCHEDULE_ATTEMPTS =
					Integer.parseInt(eElement.getElementsByTagName("max-reschedule-attempt")
							.item(0).getTextContent().trim());
			
			if (MapReduce.JobTracker.MAX_RESCHEDULE_ATTEMPTS  < 1) {
				throw new ConfFormatException ("JobTracker maximum attemps cannot be non-positive.");
			}
			
			
			/*-------------- Parse TaskTracker Expiration ----------------*/
			MapReduce.JobTracker.TASK_TRACKER_EXPIRATION =
					Integer.parseInt(eElement.getElementsByTagName("tasktracker-expiration")
							.item(0).getTextContent().trim());
			
			String unitType = 
					((Element)eElement.getElementsByTagName("tasktracker-expiration").item(0)).getAttribute("unit");
			
			int unit = parseUnit(unitType);
			
			MapReduce.JobTracker.TASK_TRACKER_EXPIRATION *= unit;
			
			if (MapReduce.JobTracker.TASK_TRACKER_EXPIRATION  < 1) {
				throw new ConfFormatException ("TaskTracker expiration cannot be non-positive.");
			}
		}
			
	}
	
	public static void mapreduceTaskTrackerCommonConf() throws ParserConfigurationException, 
	SAXException, IOException, ConfFormatException, NumberFormatException {
		
		File fXmlFile = new File("./conf/mapreduce.xml");
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
		Document doc = dBuilder.parse(fXmlFile);
		doc.getDocumentElement().normalize();
		
		NodeList nList = doc.getElementsByTagName("tasktracker");
		
		Node nNode = nList.item(0);
		
		if (nNode.getNodeType() == Node.ELEMENT_NODE) {
			
			Element eElement = (Element) nNode;
			
			nList = eElement.getChildNodes();
			
			Node commonNode = null;
			for (int i = 0; i < nList.getLength(); i++) {
				if (nList.item(i).getNodeName().equals("common")) {
					commonNode = nList.item(i);
				}
			}
			
			if (commonNode == null) {
				throw new ConfFormatException("Format error. Cannot find the <common> tag");
			}
			
			if (commonNode.getNodeType() == Node.ELEMENT_NODE) {
				
				Element commonElement = (Element) commonNode;
				
				/*-------------- Parse Temporary Directory ----------------*/
				MapReduce.TaskTracker.Common.TEMP_FILE_DIR =
						commonElement.getElementsByTagName("tmp-dir")
								.item(0).getTextContent().trim();
				
				if (MapReduce.TaskTracker.Common.TEMP_FILE_DIR == null) {
					MapReduce.TaskTracker.Common.TEMP_FILE_DIR = "tmp";
				}	
			}
			
			/*-------------- Parse Heart Beat Interval ----------------*/
			
			MapReduce.TaskTracker.Common.HEART_BEAT_FREQ =
					Integer.parseInt(eElement.getElementsByTagName("heartbeat-freq")
							.item(0).getTextContent().trim());
			
			String unitType = 
					((Element)eElement.getElementsByTagName("heartbeat-freq").item(0)).getAttribute("unit");
			
			int unit = parseUnit(unitType);
			
			MapReduce.TaskTracker.Common.HEART_BEAT_FREQ *= unit;
			
			if (MapReduce.TaskTracker.Common.HEART_BEAT_FREQ  < 1) {
				throw new ConfFormatException ("TaskTracker expiration cannot be non-positive.");
			}
		}
	}
	
	public static void mapreduceTaskTrackerIndividualConf() throws ParserConfigurationException, 
	SAXException, IOException, ConfFormatException, NumberFormatException {
		
		File fXmlFile = new File("./conf/mapreduce.xml");
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
		Document doc = dBuilder.parse(fXmlFile);
		doc.getDocumentElement().normalize();
		
		NodeList nList = doc.getElementsByTagName("tasktracker");
		
		Node nNode = nList.item(0);
		
		if (nNode.getNodeType() == Node.ELEMENT_NODE) {
			
			Element eElement = (Element) nNode;
			
			NodeList individualList = eElement.getElementsByTagName("individual");
			
			Node objectiveIndividualNode = individualList.item(0);
			
			if (objectiveIndividualNode.getNodeType() == Node.ELEMENT_NODE) {
				
				Element objectiveElement = (Element) objectiveIndividualNode;
				
				/*-------------- Parse Registry Port ----------------*/
				MapReduce.TaskTracker.Individual.TASK_TRACKER_REGISTRY_PORT =
						Integer.parseInt(objectiveElement.getElementsByTagName("registry-port")
								.item(0).getTextContent().trim());
				
				if (MapReduce.TaskTracker.Individual.TASK_TRACKER_REGISTRY_PORT   < 1024) {
					throw new ConfFormatException ("TaskTracker registry port cannot use a well-known port.");
				} else if (MapReduce.TaskTracker.Individual.TASK_TRACKER_REGISTRY_PORT   > 65535){
					throw new ConfFormatException (String.format("TaskTracker regsitry port(%d) cannot be larger than 65535."
							, MapReduce.TaskTracker.Individual.TASK_TRACKER_REGISTRY_PORT));
				}
				
				
				/*-------------- Parse Server Port ----------------*/
				MapReduce.TaskTracker.Individual.TASK_TRACKER_SERVER_PORT =
						Integer.parseInt(objectiveElement.getElementsByTagName("server-port")
								.item(0).getTextContent().trim());
				
				if (MapReduce.TaskTracker.Individual.TASK_TRACKER_SERVER_PORT   < 1024) {
					throw new ConfFormatException ("TaskTracker server port cannot use a well-known port.");
				} else if (MapReduce.TaskTracker.Individual.TASK_TRACKER_SERVER_PORT   > 65535){
					throw new ConfFormatException (String.format("TaskTracker server port(%d) cannot be larger than 65535."
							, MapReduce.TaskTracker.Individual.TASK_TRACKER_SERVER_PORT));
				}
				
				/*-------------- Parse Core Numbers ----------------*/
				MapReduce.TaskTracker.Individual.CORE_NUM =
						Integer.parseInt(eElement.getElementsByTagName("core-num")
								.item(0).getTextContent().trim());
				
				if (MapReduce.TaskTracker.Individual.CORE_NUM  < 1) {
					throw new ConfFormatException ("TaskTracker core number must be at least 1.");
				}
			}
			
			
		}
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
			unit = 1000 * 60;
		} else {
			unit = 1;
		}
		return unit;
	}
	
	public static void printConf(ConfOpt[] args) {
		for (ConfOpt arg : args) {
			if (arg == ConfOpt.HDFSCORE) {
				printHDFSCoreConf();
			} else if (arg == ConfOpt.DATANODE){
				printHDFSDataNodeConf();
			} else if (arg == ConfOpt.MAPREDCORE){
				printMapRedCoreConf();
			} else if (arg == ConfOpt.JOBTACKER) {
				printMapRedJobTrackerConf();
			} else if (arg == ConfOpt.TASKTRACKERCOMMON) {
				printMapRedTaskTrackerCommonConf();
			} else if (arg == ConfOpt.TASKTRACKERINDIVIDUAL) {
				printMapRedTaskTrackerIndividualConf();
			}
		}
		
	}
	
	
	
	public static void printHDFSCoreConf() {
		System.out.println("HDFS Core:");
		System.out.println("HDFS.Core.REPLICA_FACTOR = " + Hdfs.Core.REPLICA_FACTOR);
		System.out.println("HDFS.Core.CHUNK_SIZE = " + Hdfs.Core.CHUNK_SIZE);
		System.out.println("HDFS.Core.WRITE_BUFF_SIZE = " + Hdfs.Core.WRITE_BUFF_SIZE);
		System.out.println("HDFS.Core.READ_BUFF_SIZE = " + Hdfs.Core.READ_BUFF_SIZE);
		System.out.println("HDFS.Core.PARTITION_TOLERANCE = " + Hdfs.Core.PARTITION_TOLERANCE);
		System.out.println("HDFS.Core.NAME_NODE_IP = " + Hdfs.Core.NAME_NODE_IP);
		System.out.println("Hdfs.Core.NAME_NODE_REGISTRY_PORT = " + Hdfs.Core.NAME_NODE_REGISTRY_PORT);
		System.out.println("\n");
	}
	
	private static void printHDFSDataNodeConf() {
		System.out.println("HDFS DataNode:");
		System.out.println("HDFS.DataNode.REGISTRY_PORT = " + Hdfs.DataNode.DATA_NODE_REGISTRY_PORT);
		System.out.println("\n");
	}
	
	
	private static void printMapRedCoreConf() {
		System.out.println("MapReduce Core:");
		System.out.println("MapReduce.Core.JOB_TRACKER_IP = " + MapReduce.Core.JOB_TRACKER_IP);
		System.out.println("MapReduce.Core.JOB_TRACKER_REGISTRY_PORT = " + MapReduce.Core.JOB_TRACKER_REGISTRY_PORT);
		System.out.println("\n");
	}
	
	private static void printMapRedJobTrackerConf() {
		System.out.println("MapReduce JobTracker:");
		System.out.println("MapReduce.JobTracker.MAX_RESCHEDULE_ATTEMPTS = " + MapReduce.JobTracker.MAX_RESCHEDULE_ATTEMPTS);
		System.out.println("MapReduce.JobTracker.TASK_TRACKER_EXPIRATION = " + MapReduce.JobTracker.TASK_TRACKER_EXPIRATION);
		System.out.println("\n");
	}
	
	private static void printMapRedTaskTrackerCommonConf() {
		System.out.println("MapReduce TaskTracker Common:");
		System.out.println("MapReduce.TaskTracker.Common.HEART_BEAT_FREQ = " + MapReduce.TaskTracker.Common.HEART_BEAT_FREQ);
		System.out.println("MapReduce.TaskTracker.Common.TEMP_FILE_DIR = " + MapReduce.TaskTracker.Common.TEMP_FILE_DIR);
		System.out.println("\n");
	}
	
	private static void printMapRedTaskTrackerIndividualConf() {
		System.out.println("MapReduce TaskTracker Individual:");
		System.out.println("MapReduce.TaskTracker.Individual.TASK_TRACKER_REGISTRY_PORT = " + MapReduce.TaskTracker.Individual.TASK_TRACKER_REGISTRY_PORT);
		System.out.println("MapReduce.TaskTracker.Individual.TASK_TRACKER_SERVER_PORT = " + MapReduce.TaskTracker.Individual.TASK_TRACKER_SERVER_PORT);
		System.out.println("MapReduce.TaskTracker.Individual.CORE_NUM = " + MapReduce.TaskTracker.Individual.CORE_NUM);
		System.out.println("\n");
	}
	
	public static class ConfFormatException extends Exception {
		
		/**
		 * 
		 */
		private static final long serialVersionUID = 8494918500253529934L;

		public ConfFormatException(String message) {
			super(message);
		}
	}
	
	public enum ConfOpt {
		HDFSCORE, DATANODE, MAPREDCORE, JOBTACKER, TASKTRACKERCOMMON, TASKTRACKERINDIVIDUAL;
	}
}