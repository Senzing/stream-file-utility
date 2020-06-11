package com.senzing.utility.file.stream.communication;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.senzing.listener.senzing.service.exception.ServiceExecutionException;

public class RabbitMQProducerService {

  Connection connection = null;
  Channel channel = null;
  String mqQueue = null;

  /**
   * Initialize the producer.
   * 
   * @param mqHost Name or IP address of RabbitMQ host
   * @param mqQueue Name of queue receiving messages
   * @param mqUser User name for RabbitMQ
   * @param mqPassword Password for RabbitMQ
   * 
   * @throws IOException
   * @throws TimeoutException
   */
  public void init(String mqHost, String mqQueue, String mqUser, String mqPassword) throws IOException, TimeoutException {
    this.mqQueue = mqQueue;
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost(mqHost);
    if (mqUser != null && !mqUser.isEmpty()) {
      factory.setUsername(mqUser);
      factory.setPassword(mqPassword);
    }
    connection = factory.newConnection();
    channel = getChannel(connection, mqQueue);
  }

  /**
   * Send messages to queue
   * 
   * @param message
   * 
   * @throws ServiceExecutionException
   */
  public void send(String message) throws ServiceExecutionException {
    try {
      channel.basicPublish("", mqQueue, null, message.getBytes());
    } catch (IOException e) {
      throw new ServiceExecutionException(e);
    }
  
  }

  /**
   * Close connections
   * 
   * @throws IOException
   * @throws TimeoutException
   */
  public void close() throws IOException, TimeoutException {
    channel.close();
    connection.close();
  }

  private Channel getChannel(Connection connection, String queueName) throws IOException {
    try {
      return declareQueue(connection, queueName, true, false, false, null);
    } catch (IOException e) {
      // Possibly the queue is already declared and as non-durable. Retry with durable = false.
      return declareQueue(connection, queueName, false, false, false, null);
    }
  }

  private Channel declareQueue(Connection connection, String queueName, boolean durable, boolean exclusive,
          boolean autoDelete, Map<String, Object> arguments) throws IOException {
    Channel channel = connection.createChannel();
    channel.queueDeclare(queueName, durable, exclusive, autoDelete, arguments);
    return channel;
  }
}
