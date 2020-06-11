package com.senzing.utility.file.stream.communication;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.StringReader;
import java.util.Scanner;
import java.util.concurrent.BlockingQueue;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;

import com.senzing.listener.senzing.service.exception.ServiceExecutionException;
import com.senzing.listener.senzing.service.exception.ServiceSetupException;
import com.senzing.listener.senzing.service.g2.G2Service;

public class FileService implements Runnable {

  private static final String SEPARATOR = ",";
  private static final String DOUBLE_QUOTE = "\"";
  public static final String EOF_TAG = "EOF";

  private G2Service g2Service;
  private String fileName;
  private BlockingQueue<String> queue;

  public FileService(String dataFile, String iniFile, BlockingQueue<String> queue) throws ServiceSetupException {
    if (!(new File(dataFile).isFile())) {
      throw new ServiceSetupException("File not found: " + dataFile);
    }
    this.queue = queue;
    g2Service = new G2Service();
    g2Service.init(iniFile);
    fileName = dataFile;
  }

  @Override
  public void run() {
    try {
      consumeFile();
    } catch (ServiceExecutionException e) {
      e.printStackTrace();
    }    
  }

  /*
   * Reads lines from CSV file, generates messages based on the file information
   * and puts them on a queue.
   */
  public void consumeFile() throws ServiceExecutionException {
    Scanner sc = null;
    try {
      File file = new File(fileName);
      sc = new Scanner(file);

      while (sc.hasNextLine()) {
        String message = sc.nextLine();
        if (message.isEmpty()) {
          continue;
        }
        try {
          String martMessage = null;
          // Check if more than one field in the line (only 2 first are used if more than one).
          if (message.contains(SEPARATOR)) {
            // Expecting dsrc code, record_id.
            String[] fields = message.split(SEPARATOR);
            String dsrcCode = removeSurroundingQuotes(fields[0]);
            String recordID = removeSurroundingQuotes(fields[1]);
            martMessage = generateMartMessage(dsrcCode, recordID);
          } else {
            // Expecting entity ID when one field.
            long entityID = Long.parseLong(message);
            martMessage = generateMartMessage(entityID);
          }
          if (martMessage != null && !martMessage.isEmpty()) {
            queue.put(martMessage);
          }
        } catch (NumberFormatException | InterruptedException e) {
          throw new ServiceExecutionException(e);
        }
      }
      queue.put(EOF_TAG);
    } catch (FileNotFoundException e) {
      throw new ServiceExecutionException(e);
    } catch (InterruptedException e) {
      throw new ServiceExecutionException(e);
    } finally {
      g2Service.cleanUp();
      if (sc != null) {
        sc.close();
      }
    }
  }

  /*
   * Queries G2 for entity using source code and record id. That entity data is used to
   * generate a JSON message.
   */
  private String generateMartMessage(String dsrcCode, String recordID) throws ServiceExecutionException {
    String g2Entity = null;
    try {
      g2Entity = g2Service.getEntity(dsrcCode, recordID);
      if (g2Entity != null) {
        g2Entity = g2Entity.trim();
        if (!g2Entity.startsWith("{")) {
          System.err.println("Invalid message received from G2 for DSRC_CODE " + dsrcCode + " and RECORD_ID " + recordID);
          return "";
        }
      } else {
        return "";
      }
    } catch (ServiceExecutionException e) {
      if (e.getMessage().contains("Unknown record")) {
        System.err.println("Entity not found for DSRC_CODE " + dsrcCode + " and RECORD_ID " + recordID);
        return "";
      } else {
        throw e;
      }
    }
    return generateMartMessage(g2Entity);
  }

  /*
   * Queries G2 for entity using entity ID. That entity data is used to generate a JSON message.
   */
  private String generateMartMessage(long entityID) throws ServiceExecutionException {
    // Build messages as:
    // {"DATA_SOURCE":"TEST","RECORD_ID":"RECORD1","AFFECTED_ENTITIES":[{"ENTITY_ID":1,"LENS_CODE":"DEFAULT"}]} 
    String g2Entity = null;
    try {
      g2Entity = g2Service.getEntity(entityID, false, false);
      if (g2Entity != null) {
        g2Entity = g2Entity.trim();
      } else {
        return buildMessage(String.valueOf(entityID), "", "", "");
      }
    } catch (ServiceExecutionException e) {
      if (e.getMessage().contains("Unknown resolved entity value")) {
        System.err.println("Entity not found for ID " + entityID);
        return buildMessage(String.valueOf(entityID), "", "", "");
      } else {
        throw e;
      }
    }

    if (!g2Entity.startsWith("{")) {
      System.err.println("Invalid message received from G2 for entity: " + entityID);
      System.err.println("Message: " + g2Entity);
      return buildMessage(String.valueOf(entityID), "", "", "");
    }
    return generateMartMessage(g2Entity);
  }

  /*
   * Generates a message of the format 
   * {"DATA_SOURCE":"TEST","RECORD_ID":"RECORD1","AFFECTED_ENTITIES":[{"ENTITY_ID":1,"LENS_CODE":"DEFAULT"}]}
   */
  private String generateMartMessage(String g2Entity) throws ServiceExecutionException {
    // Build messages as:
    // {"DATA_SOURCE":"TEST",  "RECORD_ID":"RECORD1","AFFECTED_ENTITIES":[{"ENTITY_ID":1,"LENS_CODE":"DEFAULT"}]} 
    try {
      JsonReader reader = Json.createReader(new StringReader(g2Entity));
      JsonObject rootObject = reader.readObject();
      JsonObject entity = rootObject.getJsonObject("RESOLVED_ENTITY");
      String entityId = entity.getJsonNumber("ENTITY_ID").toString();
      String lensCode = entity.getString("LENS_CODE");
      JsonObject record1 = entity.getJsonArray("RECORDS").getJsonObject(0);
      String recordSource = record1.getString("DATA_SOURCE");
      String recordId = record1.getString("RECORD_ID");
      return buildMessage(entityId, recordSource, recordId, lensCode);
    } catch (RuntimeException e) {
      System.err.println("Failed to process message received from G2");
      System.err.println("Message: " + g2Entity);
      throw new ServiceExecutionException(e);
    }
  }

  private String buildMessage(String entityID, String recordSource, String recordId, String lensCode) {
    StringBuilder msgString = new StringBuilder()
        .append('{')
        .append(generateJSONAssignment("DATA_SOURCE", recordSource, true))
        .append(',')
        .append(generateJSONAssignment("RECORD_ID", recordId, true))
        .append(',')
        .append('"').append("AFFECTED_ENTITIES").append('"')
        .append(':')
        .append('[')
        .append('{')
        .append(generateJSONAssignment("ENTITY_ID", entityID, false))
        .append(',')
        .append(generateJSONAssignment("LENS_CODE", lensCode, true))
        .append('}')
        .append(']')
        .append('}');
    return msgString.toString();

  }
  private String generateJSONAssignment(String key, String value, boolean quoted) {
    StringBuilder retVal = new StringBuilder();
    retVal.append('"').append(key).append('"').append(':');
    if (quoted)
      retVal.append('"');
    retVal.append(value);
    if (quoted)
      retVal.append('"');
    return retVal.toString();
  }

  private String removeSurroundingQuotes(String source) {
    String trimmed = source.trim();
    int startPos = trimmed.startsWith(DOUBLE_QUOTE) ? 1 : 0;
    int endPos = trimmed.endsWith(DOUBLE_QUOTE) ? trimmed.length() - 1 : trimmed.length();
    return trimmed.substring(startPos, endPos);
  }
}
