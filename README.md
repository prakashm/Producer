Install Kafka in RHEL
1. Install Java (if not exist)
	sudo dnf install java-latest-openjdk -y
2. Check the Java version (should be 17+)
	java -version
3. Download kafka from below URL. Download recommended version, i downloaded 2.13
	https://kafka.apache.org/downloads
4. Add Kafka to system path
	mv kafka_2.13-3.5.1/ kafka/
	echo "export PATH=$PATH:$HOME/kafka/bin" >> ~/.bash_profile
	source ~/.bash_profile
5. Open the new terminal and start the zoo keeper. it will start in localhost:2181
	sh zookeeper-server-start.sh $HOME/kafka/config/zookeeper.properties
6. Test the zookeeper using telnet
	telnet localhost 2181
7. Open another terminal and start the Apache kafka server
	sh kafka-server-start.sh $HOME/kafka/config/server.properties
8. Test the Kafka server using telnet
	telnet localhost 9092
