package com.datatorrent.spark.launcher;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.spark.launcher.SparkLauncher;
import org.apache.spark.streaming.Milliseconds;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * Created by bhupesh on 16/3/18.
 */
public class SparkAppLauncher
{

  public void launch() throws IOException, InterruptedException
  {
    Process spark = new SparkLauncher()
      .setSparkHome("/home/bhupesh/spark/spark-2.2.1-bin-hadoop2.6")
      .setAppResource("/code/dev/spark-apps/target/spark-apps-1.0-SNAPSHOT-jar-with-dependencies.jar")
      .setMainClass("com.datatorrent.spark.apps.SparkAppUsingConnectors")
      .setMaster("yarn")
      .setDeployMode("cluster")
      .launch();

    InputStreamReaderRunnable inputStreamReaderRunnable = new InputStreamReaderRunnable(spark.getInputStream(), "input");
    Thread inputThread = new Thread(inputStreamReaderRunnable, "LogStreamReader input");
    inputThread.start();

    InputStreamReaderRunnable errorStreamReaderRunnable = new InputStreamReaderRunnable(spark.getErrorStream(), "error");
    Thread errorThread = new Thread(errorStreamReaderRunnable, "LogStreamReader error");
    errorThread.start();

    System.out.println("Waiting for finish...");
    int exitCode = spark.waitFor();
    System.out.println("Finished! Exit code:" + exitCode);

  }

  public static void main(String[] args) throws IOException, InterruptedException
  {
    SparkAppLauncher sparkAppLauncher = new SparkAppLauncher();
    sparkAppLauncher.launch();
  }

  public static class InputStreamReaderRunnable implements Runnable {

    private BufferedReader reader;

    private String name;

    public InputStreamReaderRunnable(InputStream is, String name) {
      this.reader = new BufferedReader(new InputStreamReader(is));
      this.name = name;
    }

    public void run() {
      System.out.println("InputStream " + name + ":");
      try {
        String line = reader.readLine();
        while (line != null) {
          System.out.println(line);
          line = reader.readLine();
        }
        reader.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }
}
