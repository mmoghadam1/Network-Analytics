package edu.cu.boulder.cs.flink.triangles;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.time.Instant;

/**
 * Class representing a temporal edge.  It has a source, target, and timestamp.
 */
public class TemporalEdge {
  public int source;
  public int target;
  double time;

  TemporalEdge(int source, int target, double time) {
    this.source = source;
    this.target = target;
    this.time   = time;
  }

  public String toString()
  {
    return time + ", " + source + ", " + target;
  }
}
