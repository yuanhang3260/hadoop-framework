#Building A MapReduce Facility
==============================

##1. Objective

In this project, we are implementing a MapReduce facility like Hadoop which can process data in parallel by exploiting the independency of data.

##2. System Design

Our MapReduce framework consists of two major parts: (1) distributed file system(DFS) and (2) MapReduce framework. Our DFS provides a global access to file where several replicas are stored in different nodes. And the MapReduce framework divides the data into chunks and processes each of in parallel to achieve high performance.

###2.1 Distributed File System(DFS)

General Description

Our DFS consists of two major parts: (1) NameNode and (2) DataNode. NameNode manages files metadata, periodically checks the status of DataNodes and files. DataNode is where data is stored and reports its overall availability.

The DFS implements a 

Each file in DFS is denoted as an HDFSFile whose metadata is managed by NameNode. Each HDFSFile consists of file name, a list of chunks into which a huge file is divided, replica factor that describes the enforced replication amount for each file, availability of a file. 

HDFSChunk is the abstraction of chunks. Each HDFSChunk keeps the chunk name and the locations for each of replicas. The chunk name is the file name recognized by DataNode's file system. HDFSChunk's chunksize is configurable.

The DFS is customized for our MapReduce and random access is not supported. Unlike Hadoop Distirbuted File System(HDFS) which chops large data into equal size chunks, we divides a file into chunks with end of a new line feed except the last chunk. As our MapReduce framework is customized for processing data line by line, each chunk is divided by a new line feed so that each chunk can be fed to a Mapper task at a time and no more further data traffic is needed between different chunks during the MapReduce. This customization leaves the little difference of length among chunks but is leaves a decent interface to MapReduce.
