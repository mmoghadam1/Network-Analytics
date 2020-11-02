package edu.cu.boulder.cs.flink.triangles;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.io.*;
import java.time.Instant;
import java.util.Random;
import java.io.File; 
import java.util.Scanner; 
import java.io.FileNotFoundException;

/**
 * This creates netflows from a pool of IPs.
 */
public class NetflowSource extends RichParallelSourceFunction<Netflow> {

  /// The number of netflows to create.

  private String filename;
  public String sourceIp;
  public String destIp;
  /**
   *
   * @param numEvents The number of netflows to produce.
   * @param numIps The number of ips in the pool.
   * @param rate The rate of netflows produced in netflows/s.
   */
  public NetflowSource(String filename)
  {
    this.filename = filename;
    System.out.print("we are in the constructor");    
  }

  @Override
  public void run(SourceContext<Netflow> out) throws Exception
  {
    RuntimeContext context = getRuntimeContext();
    int taskId = context.getIndexOfThisSubtask();
    try {
      File myObj = new File(this.filename);
      Scanner myReader = new Scanner(myObj);
      while (myReader.hasNextLine()) {
	String data = myReader.nextLine();
	String[] flowFields = data.split(",");
	long timestamp = Long.valueOf(flowFields[0]);
        String sourceIP = flowFields[10];
        String destIP = flowFields[11];
        Netflow netflow = new Netflow(timestamp, sourceIP, destIP);
        System.out.println(netflow);
	out.collectWithTimestamp(netflow, timestamp);
        out.emitWatermark(new Watermark(timestamp));
      }
      myReader.close();
    } 
    catch (FileNotFoundException e) {
      System.out.println("An error occurred.");
      e.printStackTrace();
    };
  }
  @Override
  public void cancel()
  {
  }
}
