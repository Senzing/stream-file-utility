package com.senzing.utility.file.stream;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeoutException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.senzing.listener.senzing.data.ConsumerCommandOptions;
import com.senzing.listener.senzing.service.exception.ServiceExecutionException;
import com.senzing.listener.senzing.service.exception.ServiceSetupException;
import com.senzing.utility.file.stream.communication.FileService;
import com.senzing.utility.file.stream.communication.RabbitMQProducerService;

public class StreamUtilityApp {

  private static String dataFile;
  private static String iniFile;
  private static String mqHost;
  private static String mqQueue;
  private static String mqUser;
  private static String mqPassword;

  private static final String INI_FILE = "iniFile";
  private static final String DATA_FILE = "dataFile";

  public static void main(String[] args) {

    try {
      processArguments(args);
    } catch (Exception e) {
      helpMessage();
      e.printStackTrace();
      System.exit(-1);
    }

    RabbitMQProducerService mqService = null;
    try {
      mqService = new RabbitMQProducerService();
      mqService.init(mqHost, mqQueue, mqUser, mqPassword);

      BlockingQueue<String> blockingQueue = new ArrayBlockingQueue<>(1000);

      FileService fileService = new FileService(dataFile, iniFile, blockingQueue);
      Thread thread = new Thread(fileService);
      thread.start();

      long count = 0;
      String message;
      while ((message = blockingQueue.take()) != "EOF") {
        mqService.send(message);
        count++;
        if (count % 1000 == 0) {
          System.out.println("Messages processed: " + count);
        }
      }
      System.out.println("Total messages processed: " + count);
    } catch (ServiceSetupException | IOException | TimeoutException | InterruptedException
        | ServiceExecutionException e) {
      e.printStackTrace();
    } finally {
      if (mqService != null) {
        try {
          mqService.close();
        } catch (IOException | TimeoutException e) {
          e.printStackTrace();
        }
      }
    }
  }

  /*
   * Processes command line arguments and assigns their values to variables.
   */
  private static void processArguments(String[] args) throws ParseException {
    Options options = new Options();
    options.addOption(INI_FILE, true, "Path to the G2 ini file");
    options.addOption(ConsumerCommandOptions.MQ_HOST, true, "Host for RabbitMQ");
    options.addOption(ConsumerCommandOptions.MQ_USER, true, "User name for RabbitMQ");
    options.addOption(ConsumerCommandOptions.MQ_PASSWORD, true, "Password for RabbitMQ");
    options.addOption(ConsumerCommandOptions.MQ_QUEUE, true, "Queue name for the receiving queue");
    options.addOption(DATA_FILE, true, "Path to source file");

    CommandLineParser parser = new DefaultParser();
    CommandLine commandLine = parser.parse(options, args);

    dataFile = getCommandArgumentValue(commandLine, DATA_FILE);
    mqHost = getCommandArgumentValue(commandLine, ConsumerCommandOptions.MQ_HOST);
    mqUser = getCommandArgumentValue(commandLine, ConsumerCommandOptions.MQ_USER);
    mqPassword = getCommandArgumentValue(commandLine, ConsumerCommandOptions.MQ_PASSWORD);
    mqQueue = getCommandArgumentValue(commandLine, ConsumerCommandOptions.MQ_QUEUE);
    iniFile = getCommandArgumentValue(commandLine, INI_FILE);

    if (dataFile == null || mqHost == null || mqQueue == null || iniFile == null) {
      helpMessage();
      System.exit(-1);
    }
  }

  private static String getCommandArgumentValue(CommandLine commandLine, String key) {
    if (commandLine.hasOption(key)) {
      return commandLine.getOptionValue(key);
    }
    return null;
  }

  private static void helpMessage() {
    System.out.println(
        "Usage: java -jar stream-utility.jar -dataFile <path to source file> -iniFile <path to ini file> -mqQueue <name of the queue read from> -mqHost <host name for queue server> [-mqUser <queue server user name>] [-mqPassword <queue server password>]");
  }
}
