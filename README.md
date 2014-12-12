DistributedPubSub
=================

A simple pub-sub implementation with a single server.

Depends on:
* Typesafe Config
* Protocol Buffers
* ZeroMQ


REQUIRED INSTALLATION:

* Protocol Buffers 2.5.0
 	- Note that the protocol buffers version provided by your operating system might be an earlier version
	- Install protocol buffers 2.5.0 http://code.google.com/p/protobuf/downloads/list
	
		```	
		apt-get remove protobuf-compiler
		curl -# -O https://protobuf.googlecode.com/files/protobuf-2.5.0.tar.gz
		gunzip protobuf-2.5.0.tar.gz
		tar -xvf protobuf.2.5.0.tar
		cd protobuf-2.5.0
		./configure --prefix=/usr
		make
		make install
		```
	- Ensure the protoc executable is on your PATH (test with 'protoc --version')
	

	
OPTIONAL INSTALLATION

* Zero MQ
	Installation of zero MQ and the java language bindings is optional; can use native bindings (faster)
	or pure java implementation
	- Ubuntu:
		* Install zero MQ core http://zeromq.org/distro:debian
		* Install java zero MQ http://zeromq.org/bindings:java
	- Windows:
		* Install zero MQ core http://zeromq.org/distro:microsoft-windows
		* Install java zero MQ http://zeromq.org/bindings:java


BUILDING THIS PACKAGE

*** By default, will NOT use native bindings; will choose pure java implementation ***
mvn clean package install

To install using native bindings:
mvn clean package install -Pnative
