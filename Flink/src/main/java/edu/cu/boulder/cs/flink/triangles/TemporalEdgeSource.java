package edu.cu.boulder.cs.flink.triangles;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.time.Instant;
import java.util.Random;

public class TemporalEdgeSource extends RichParallelSourceFunction<TemporalEdge>
{
  /// The number of edges to create.
  private int numEdges;

  /// How many edges created.
  private int currentEdge;

  /// How many unique vertices
  private int numVertices;

  /// Used to create the netflows with randomly selected ips
  private Random random;

  /// Time from start of run
  private double time = 0;

  /// The time in seconds between netflows
  private double increment;

  /// When the first netflow was created
  private double t1;

  public TemporalEdgeSource(int numEdges, int numVertices, double rate) {
    this.numEdges = numEdges;
    currentEdge = 0;
    this.numVertices = numVertices;
    this.increment = 1 / rate;
  }

  @Override
  public void run(SourceFunction.SourceContext<TemporalEdge> out) throws Exception
  {

    if (currentEdge == 0) {
      int index = getRuntimeContext().getIndexOfThisSubtask();
      this.random = new Random(index);
      System.out.println("index " + index);
      t1 = ((double) Instant.now().toEpochMilli()) / 1000;
    }

    while(currentEdge < numEdges)
    {
      long t = Instant.now().toEpochMilli();
      double currentTime = ((double)t) / 1000;
      if (currentEdge % 1000 == 0) {
        double expectedTime = currentEdge * increment;
        double actualTime = currentTime - t1;
        System.out.println("Expected time: " + expectedTime +
            "Actual time: " + actualTime);
      }

      double diff = currentTime - t1;
      if (diff < currentEdge * increment)
      {
        long numMilliseconds = (long)(currentEdge * increment - diff) * 1000;
        Thread.sleep(numMilliseconds);
      }

      int source = random.nextInt(numVertices);
      int target = random.nextInt(numVertices);

      TemporalEdge edge = new TemporalEdge(source, target, time);

      out.collectWithTimestamp(edge, t);
      out.emitWatermark(new Watermark(t));

      currentEdge++;
      time += increment;
    }


    long t = Instant.now().toEpochMilli();
  }

  @Override
  public void cancel()
  {
    this.currentEdge = numEdges;
  }
}
