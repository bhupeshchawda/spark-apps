package com.datatorrent.spark.launcher;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.util.Map;

import org.apache.spark.launcher.MyLauncher;
import org.apache.spark.launcher.SparkAppHandle;

import com.google.common.collect.Maps;

/**
 * Created by bhupesh on 16/3/18.
 */
public class SparkTestAppLauncher
{
  public void launch() throws IOException, InterruptedException
  {
    Map<String, String> env = Maps.newHashMap();
    env.put("SPARK_PRINT_LAUNCH_COMMAND", "true");
    MyLauncher launcher = new MyLauncher(env);
    SparkAppHandle spark = launcher
      .setSparkHome("/home/bhupesh/spark/spark-2.3.0-bin-hadoop2.7")
      .setAppResource("/code/dev/spark-apps/target/spark-apps-1.0-SNAPSHOT-jar-with-dependencies.jar")
      .setMainClass("com.datatorrent.spark.apps.SparkTestApp")
      .setMaster("local[*]")
      .setDeployMode("client")
      .setConf("spark.executor.extraClassPath", "/code/dev/spark-apps/target/spark-apps-1.0-SNAPSHOT-jar-with-dependencies.jar")
      .setConf("spark.metrics.conf", "/home/bhupesh/spark/spark-2.3.0-bin-hadoop2.7/conf/metrics.properties")
      .addFile("/home/bhupesh/spark/spark-2.3.0-bin-hadoop2.7/conf/metrics.properties")
      .startApplication(new SparkAppHandle.Listener()
      {
        @Override
        public void stateChanged(SparkAppHandle handle)
        {
          System.out.println("State: " + handle.getState());
        }

        @Override
        public void infoChanged(SparkAppHandle handle)
        {
          System.out.println("State: " + handle.getState());
        }
      });

//    InputStreamReaderRunnable inputStreamReaderRunnable = new InputStreamReaderRunnable(spark.getInputStream(), "input");
//    Thread inputThread = new Thread(inputStreamReaderRunnable, "LogStreamReader input");
//    inputThread.start();
//
//    InputStreamReaderRunnable errorStreamReaderRunnable = new InputStreamReaderRunnable(spark.getErrorStream(), "error");
//    Thread errorThread = new Thread(errorStreamReaderRunnable, "LogStreamReader error");
//    errorThread.start();

    System.out.println("Waiting for finish...");

    System.out.println(spark.getState());

    Thread.sleep(30000);
    System.out.println(spark.getAppId());
    System.out.println(spark.getState());
    System.out.println(getPidOfProcess(launcher.process));
    Thread.sleep(10000);
    System.out.println(spark.getAppId());
    System.out.println(spark.getState());
    spark.kill();

//    spark.stop();


//    System.out.println(spark.getState());
//    int exitCode = spark.waitFor();
//    System.out.println("Finished! Exit code:" + exitCode);
  }

  public static synchronized long getPidOfProcess(Process p)
  {
    long pid = -1;

    try {
      if (p.getClass().getName().equals("java.lang.UNIXProcess")) {
        Field f = p.getClass().getDeclaredField("pid");
        f.setAccessible(true);
        pid = f.getLong(p);
        f.setAccessible(false);
      }
    } catch (Exception e) {
      pid = -1;
    }
    return pid;
  }


  public static void main(String[] args) throws IOException, InterruptedException
  {
    SparkTestAppLauncher sparkAppLauncher = new SparkTestAppLauncher();
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
