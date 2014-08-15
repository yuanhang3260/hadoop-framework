#Building A MapReduce Facility
==============================

##1. Objective

In this project, we are implementing a MapReduce facility like Hadoop which can process data in parallel by exploiting the independency of data.

##2. System Design

Our MapReduce framework consists of two major parts: (1) distributed file system(DFS) and (2) MapReduce framework. Our DFS provides a global access to file where several replicas are stored in different nodes. And the MapReduce framework divides the data into chunks and processes each of in parallel to achieve high performance.

###2.1 Distributed File System(DFS)

####General Description

Our DFS consists of two major parts: (1) NameNode and (2) DataNode. NameNode manages files metadata, periodically checks the status of DataNodes and files. DataNode is where data is stored and reports its overall availability to NameNode.

Each file in DFS is denoted as an HDFSFile whose metadata is managed by NameNode. Each HDFSFile consists of file name, a list of chunks into which a huge file is divided, replica factor that describes the enforced replication amount for each file, availability of a file. 

HDFSChunk is the abstraction of chunks. Each HDFSChunk keeps the chunk name and the locations for each of replicas. The chunk name is the file name recognized by DataNode's file system. HDFSChunk's chunksize is configurable.

The DFS is customized for our MapReduce and random access is not supported. Unlike Hadoop Distirbuted File System(HDFS) which chops large data into equal size chunks, we divides a file into chunks with end of a new line feed except the last chunk. As our MapReduce framework is customized for processing data line by line, each chunk is divided by a new line feed so that each chunk can be fed to a Mapper task at a time and no more further data traffic is needed between different chunks during the MapReduce. This customization leaves the little difference of length among chunks but is leaves a decent interface to MapReduce.

####Semantics

The way the file uploaded to distributed file systems is as follows: The client request to create a file. If the file name has been used, a failure is pushed back to client. After client successfully requested a file discriptor, the client is in charge of maintaining HDFSFile objcet. The HDFSFile object contains all information of this file, such as the file name, metadata of all chunks. The HDFSOutputStream is implicitly created by getOutputStream() method instead of the constructor fo HDFSOutputStream. Now the client can use HDFSOutputStream to feed data to the distributed system. it continues writing from local to the system. The file systems won’t see the any detailed information of this file before the output stream is closed. Therefore, despite the client has already written some chunks to the file systems, the file is invisible. Once the client successfully close the output stream, the file is invisible at client. 

In our distributed system, if two different HDFSOutputStream objects write data seperately to the DFS, the file well be overlapped by file. That is to say, the version of the file depends the latest close operation. Besides, the final version of this file depends the later closed output stream.

Our distributed file systems are IP-aware, by which we believe that the smaller difference of two IPs represents the closer geographic location. By utilizing this feature, the MapReduce framework is able to process work as close to the data node as possible. As moving data is much more expensive than moving process, this helps to improve the MapReduce framework a lot. And even if the data needs to be carried around,  the traffic is well limited to a small networks because the distributed file system is IP-aware. During reading the file, the HDFSInputStream tries to find the nearest valid DataNode to retrieve chunks.

####NameNode

The NameNode is the coordinator of the DFS. It doesn’t deal with any I/O manipulation. Instead, it monitors the status of each DataNode, the file availability and enforces the robustness of each files. As it is the coordinator of the distributed file systems, only metadata of each file, chunk and DataNode is stored by NameNode. 

NameNode acts as the only naming service within the the system. As a administrative node, NameNode contains a lot of relationship information of chunks. For example, the NameNode should know the map from file name to file metadata. Besides, the NameNode should also know the metadata of each chunk of each file. It is in charge of naming each of chunk as well. The synchronized naming service is provided at NameNode only with help of timestamp and provides a decent “happens before” relationship among files.

By design, NameNode and DataNode communicates in one way via heartbeat. DataNode periodic sends heartbeat to NameNode to show its existence, NameNode passes tasks back to DataNode. This simplifies the communication complexity and also effectively prevents the dead locks.

In order to maintain the health of distributed file systems, the NameNode should do a periodic system check. This periodic system check collects the chunks information generated by each DataNode and compare it with the metadata kept by itself. 3 different situations are handled by NameNode: (1). the chunks which is detected by DataNodes while NameNode is unaware of its existence are called orphans. (2) Chunks that are less than the specified replica factor need to be replicated in time to improve the robustness of the total systems. (3) Chunks that are over replicated are enforced to be deleted in order to save spaces on each DataNode.

Load balance is handled by our distributed systems as well. NameNode selects the DataNodes with lightest chunks number upon clients request the destination to write data to. Even though the data is not precisely equal in length, they are regarded as the same by our system because the difference between by different chunks mostly varies tens of bytes to hundreds of bytes which is ignorable compared to each chunk.

####DataNode

The DataNode is known as storage node in our systems. DataNode stores chunks without the knowledge of which files they belong to. By design, the DataNode always initiates the communication and informing its status and take action as NameNode requests. Besides communicating with NameNode with its status, it receives task from NameNode and perform tasks. DataNode also responds to file I/O request from clients. 

DataNode often initiates two types communication with NameNode: (1) HeartBeat and (2) Chunk Report. The heartbeat informs NameNode the existence of DataNode. The chunk report sends the chunks information on the node to NameNode to enable NameNode monitoring the status of each file. Since the chunk report may be huge as the accumulative chunks, the chunk report is much less frequent than heartbeat. Between two chunk reports, NameNode keeps track of manipulation on each DataNode. Upon the chunk reports, the NameNode update the information with the help of chunk report as chunks report reflects more real-time information of each chunk. As illustrated in NameNode section, the DataNode may delete orphans, make replications and delete redundant replications upon NameNode’s request.

DataNode also exposes several remote method invocations to clients such as request chunks of files. Those invocations contain write to or read from chunks. DataNode has relatively lower computational task but a heavier I/O manipulations.

####Client

Client is a machine requesting distributed file system service. Our distributed file systems provides 4 basic service to clients: (1) create a file and write to lightest-load node. (2) open a file and read from it from closet DataNode (3) remove a file on NameNode and corresponding chunks on DataNode. (4) list all files on the systems.

![alt tag](https://raw.githubusercontent.com/Jeremy-Fu/hadoop/master/fig1.png)

To conclude, the above figure shows the general traffic between different components. NameNode takes the most heavy computation tasks. But it doesn’t take a I/O traffic burden. DataNode is in charge of I/O with client.

####Fault Tolerance

The DFS suffers from the one-point-failure. If the NameNode is partitioned by network or fails down, all services are declined.

Due to the unreliable nature of Internet, we spent efforts in dealing with network partitions. if a partition happens several results will occur:
1. Opening and creating file succeed only if the client is in the same partition as NameNode.
2. Read succeeds if at least one DataNode is in the same partition with DataNode, and NameNode doesn’t have to be in the same partition because the chunk information has all been included in HDFSFile.
3. Write may succeed if there is at least one available DataNode and the NameNode has to be in the same partition as well. We say write may succeed because it depends the destination DataNode assigned by NameNode. And NameNode may not notice the partition until a system check starts and the partition time(estimated by the difference between current time and last recent heartbeat) exceeds the partition toleration. 

If the DataNode doesn’t send heartbeat to NameNode, the NameNode believes it to be unavailable. Once the DataNode is believed to be unavailable, NameNode won’t count on chunks information reported from such DataNode. And most of time, several actions are taken subsequently. For example new replications are often made on available nodes. In extreme situation, all replicas of a chunk is missed, the file is believed to be unavailable.

