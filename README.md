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

