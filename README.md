DistributedPubSub
=================

A simple pub-sub implementation with a single server.

Depends on:
* Typesafe Config
* Protocol Buffers
* ZeroMQ


REQUIRED INSTALLATION:

* Protocol Buffers
	- Install protocol buffers 2.5.0 http://code.google.com/p/protobuf/downloads/list
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
