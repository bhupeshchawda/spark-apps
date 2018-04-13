package org.apache.spark.launcher;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;

import static org.apache.spark.launcher.CommandBuilderUtils.checkState;
import static org.apache.spark.launcher.CommandBuilderUtils.isWindows;
import static org.apache.spark.launcher.CommandBuilderUtils.quoteForBatchScript;

/**
 * Created by bhupesh on 26/3/18.
 */
public class MyLauncher extends SparkLauncher
{
  private static final AtomicInteger COUNTER = new AtomicInteger();
  public Process process;

  public MyLauncher(Map<String, String> env) {
    super(env);
  }

  @Override
  public SparkAppHandle startApplication(SparkAppHandle.Listener... listeners) throws IOException
  {
    LauncherServer server = LauncherServer.getOrCreateServer();
    ChildProcAppHandle handle = new ChildProcAppHandle(server);
    for (SparkAppHandle.Listener l : listeners) {
      handle.addListener(l);
    }

    String secret = server.registerHandle(handle);

    String loggerName = getLoggerName();
    ProcessBuilder pb = createBuilder();

    boolean outputToLog = outputStream == null;
    boolean errorToLog = !redirectErrorStream && errorStream == null;

    // Only setup stderr + stdout to logger redirection if user has not otherwise configured output
    // redirection.
    if (loggerName == null && (outputToLog || errorToLog)) {
      String appName;
      if (builder.appName != null) {
        appName = builder.appName;
      } else if (builder.mainClass != null) {
        int dot = builder.mainClass.lastIndexOf(".");
        if (dot >= 0 && dot < builder.mainClass.length() - 1) {
          appName = builder.mainClass.substring(dot + 1, builder.mainClass.length());
        } else {
          appName = builder.mainClass;
        }
      } else if (builder.appResource != null) {
        appName = new File(builder.appResource).getName();
      } else {
        appName = String.valueOf(COUNTER.incrementAndGet());
      }
      String loggerPrefix = getClass().getPackage().getName();
      loggerName = String.format("%s.app.%s", loggerPrefix, appName);
    }

    if (outputToLog && errorToLog) {
      pb.redirectErrorStream(true);
    }

    pb.environment().put(LauncherProtocol.ENV_LAUNCHER_PORT, String.valueOf(server.getPort()));
    pb.environment().put(LauncherProtocol.ENV_LAUNCHER_SECRET, secret);
    try {
      Process child = pb.start();
      process = child;
      InputStream logStream = null;
      if (loggerName != null) {
        logStream = outputToLog ? child.getInputStream() : child.getErrorStream();
      }
      handle.setChildProc(child, loggerName, logStream);
    } catch (IOException ioe) {
      handle.kill();
      throw ioe;
    }

    return handle;
  }

  private ProcessBuilder createBuilder() throws IOException {
    List<String> cmd = new ArrayList<String>();
    cmd.add(findSparkSubmit());
    cmd.addAll(builder.buildSparkSubmitArgs());

    // Since the child process is a batch script, let's quote things so that special characters are
    // preserved, otherwise the batch interpreter will mess up the arguments. Batch scripts are
    // weird.
    if (isWindows()) {
      List<String> winCmd = new ArrayList<String>();
      for (String arg : cmd) {
        winCmd.add(quoteForBatchScript(arg));
      }
      cmd = winCmd;
    }

    ProcessBuilder pb = new ProcessBuilder(cmd.toArray(new String[cmd.size()]));
    for (Map.Entry<String, String> e : builder.childEnv.entrySet()) {
      pb.environment().put(e.getKey(), e.getValue());
    }

    if (workingDir != null) {
      pb.directory(workingDir);
    }

    // Only one of redirectError and redirectError(...) can be specified.
    // Similarly, if redirectToLog is specified, no other redirections should be specified.
    checkState(!redirectErrorStream || errorStream == null,
      "Cannot specify both redirectError() and redirectError(...) ");
    checkState(getLoggerName() == null ||
        ((!redirectErrorStream && errorStream == null) || outputStream == null),
      "Cannot used redirectToLog() in conjunction with other redirection methods.");

    if (redirectErrorStream) {
      pb.redirectErrorStream(true);
    }
    if (errorStream != null) {
      pb.redirectError(errorStream);
    }
    if (outputStream != null) {
      pb.redirectOutput(outputStream);
    }

    return pb;
  }

  private String getLoggerName() throws IOException {
    return builder.getEffectiveConfig().get(CHILD_PROCESS_LOGGER_NAME);
  }


}
