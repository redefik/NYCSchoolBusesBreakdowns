echo "Java 8 Installation..."
wget --no-check-certificate --no-cookies --header "Cookie: oraclelicense=accept-securebackup-cookie" http://download.oracle.com/otn-pub/java/jdk/8u141-b15/336fa29ff2bb4ef291e347e091f7f4a7/jdk-8u141-linux-x64.rpm
sudo yum install -y jdk-8u141-linux-x64.rpm
echo "Installation completed"
echo "Kafka Installation..."
wget https://downloads.apache.org/kafka/2.5.0/kafka_2.12-2.5.0.tgz
tar -xzf kafka_2.12-2.5.0.tgz
rm kafka_2.12-2.5.0.tgz
echo "Kafka Installation completed"
echo "
To start Zookeeper:
>cd kafka_2.12-2.5.0
>nohup bin/zookeeper-server-start.sh config/zookeeper.properties > ~/zookeeper-logs &
Logout, ssh again and check zookeeper-logs
An error may have been occurred. In that case:
sudo sh -c 'echo 1 >/proc/sys/vm/drop_caches'
sudo sh -c 'echo 2 >/proc/sys/vm/drop_caches'
sudo sh -c 'echo 3 >/proc/sys/vm/drop_caches'
and retry
To start Kafka:
>cd kafka_2.12-0.10.2.0
>nohup bin/kafka-server-start.sh config/server.properties --override advertised.listeners=PLAINTEXT://<IP>:9092 > ~/kafka-logs &
Logout, ssh again and check kafka-logs "