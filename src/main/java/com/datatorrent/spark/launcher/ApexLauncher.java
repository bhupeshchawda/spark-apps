package com.datatorrent.spark.launcher;

import java.io.IOException;

import org.apache.apex.api.Launcher;
import org.apache.apex.api.YarnAppLauncher;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.lib.io.ConsoleOutputOperator;

/**
 * Created by bhupesh on 20/3/18.
 */
public class ApexLauncher
{
  public static void main(String[] args) throws IOException
  {
    YarnAppLauncher launcher = Launcher.getLauncher(Launcher.LaunchMode.YARN);
    Attribute.AttributeMap launchAttributes = new Attribute.AttributeMap.DefaultAttributeMap();
    Configuration conf = new Configuration();
    conf.addResource(new Path("/home/bhupesh/hadoop/hadoop-2.6.4/etc/hadoop/core-site.xml"));
    Launcher.AppHandle appHandle = launcher.launchApp(new SimpleApexApp(), conf, launchAttributes);
  }

  public static class SimpleApexApp implements StreamingApplication
  {
    public void populateDAG(DAG dag, Configuration configuration)
    {
      RandomGenerator gen = dag.addOperator("Input", new RandomGenerator());
      ConsoleOutputOperator output = dag.addOperator("Output", new ConsoleOutputOperator());
      dag.addStream("stream", gen.output, output.input);
    }
  }

  public static class RandomGenerator extends BaseOperator implements InputOperator
  {

    public final transient DefaultOutputPort<Integer> output = new DefaultOutputPort<Integer>();

    public void emitTuples()
    {
      output.emit(2);
      try {
        Thread.sleep(10L);
      } catch (InterruptedException e) {
        throw new RuntimeException("Exception in sleep", e);
      }
    }
  }
}
