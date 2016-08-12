# Installing Hadoop
I found an excellent [installation guide of Hadoop 2.6](http://zhongyaonan.com/hadoop-tutorial/setting-up-hadoop-2-6-on-mac-osx-yosemite.html)
by [Yaonan Zhong](http://zhongyaonan.com/) for setting up Hadoop 2.6 on Mac OS X Yosemite. I will place the content
here just for reference.

For the time being, I'm only interested in HDFS.

## Setup SSH
First enable __Remote Login__ in __System Preference -> Sharing__.

Now check that you can ssh to the localhost without a passphrase:

```
$ ssh localhost
```

If you cannot ssh to localhost without a passphrase, execute the following commands:

```
$ ssh-keygen -t dsa -P '' -f ~/.ssh/id_dsa
$ cat ~/.ssh/id_dsa.pub >> ~/.ssh/authorized_keys
```

## Installing HDFS

## 1. Get a Hadoop distribution
You can download it from [Apache Download Mirror](http://www.apache.org/dyn/closer.cgi/hadoop/common/).

## 2. Prepare to start the Hadoop cluster
1. Unpack the downloaded Hadoop distribution.

## 3. Configuration
Edit following config files in your Hadoop directory

1. `etc/hadoop/core-site.xml`:

```xml
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://localhost:9000</value>
    </property>
</configuration>
```

2. `etc/hadoop/hdfs-site.xml`:

```xml
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>1</value>
    </property>
</configuration>
```

## 4. Setup HDFS

1. Format and start HDFS

```
$ cd {your hadoop distribution directory}
```

Format the filesystem:

```
$ bin/hdfs namenode -format
```

Start NameNode daemon and DataNode daemon:

```
$ sbin/start-dfs.sh
```

Now you can browse the web interface for the NameNode at - [http://localhost:50070/](http://localhost:50070/])

Make the HDFS directories required to execute MapReduce jobs:

```
$ bin/hdfs dfs -mkdir /user
$ bin/hdfs dfs -mkdir /user/{username} #make sure you add correct username here
```

Copy the input files into the distributed filesystem:

```
$ bin/hdfs dfs -put etc/hadoop input
```

Copy the output files from the distributed filesystem to the local filesystem and examine them:

```
$ bin/hdfs dfs -get output output
$ cat output/*
```

or View the output files on the distributed filesystem:

```
$ bin/hdfs dfs -cat output/*
```

## 5. Stop HDFS

When you're done, stop the daemons with:

```
$ sbin/stop-dfs.sh
```