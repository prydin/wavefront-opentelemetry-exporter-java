package com.wavefront.opentelemetry.exporter;

import com.wavefront.sdk.common.Pair;
import com.wavefront.sdk.common.WavefrontSender;
import com.wavefront.sdk.direct.ingestion.WavefrontDirectIngestionClient;
import com.wavefront.sdk.entities.tracing.SpanLog;
import com.wavefront.sdk.proxy.WavefrontProxyClient;
import io.opentelemetry.sdk.trace.SpanData;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import io.opentelemetry.trace.AttributeValue;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.net.SocketFactory;

public class WavefrontSpanExporter implements SpanExporter {
  private static final Logger logger =
      Logger.getLogger(WavefrontSpanExporter.class.getCanonicalName());
  private final WavefrontSender sender;

  protected WavefrontSpanExporter(WavefrontSender sender) {
    this.sender = sender;
  }

  private static UUID makeUUID(String s) {
    if (s.length() >= 8) {
      return new UUID(0, Long.parseLong(s, 16));
    }
    return new UUID(
        Long.parseLong(s.substring(0, 8 - s.length()), 16), Long.parseLong(s.substring(8), 16));
  }

  private static List<Pair<String, String>> attrsToTags(Map<String, AttributeValue> attrs) {
    List<Pair<String, String>> tags = new ArrayList<>(attrs.size());
    for (Map.Entry<String, AttributeValue> attr : attrs.entrySet()) {
      tags.add(new Pair<>(attr.getKey(), attrToString(attr.getValue())));
    }
    return tags;
  }

  private static String attrToString(AttributeValue attr) {
    switch (attr.getType()) {
      case LONG:
        return Long.toString(attr.getLongValue());
      case BOOLEAN:
        return Boolean.toString(attr.getBooleanValue());
      case DOUBLE:
        return Double.toString(attr.getDoubleValue());
      case STRING:
        return attr.getStringValue();
      default:
        logger.log(Level.WARNING, "Unknown attribute type: " + attr.getType() + ". Skipping!");
        return null;
    }
  }

  @Override
  public ResultCode export(List<SpanData> spans) {
    for (SpanData span : spans) {
      System.out.println("******** SPAN: " + span.getName());
      List<SpanLog> spanLogs = new ArrayList<>(spans.size());
      for (SpanData.TimedEvent event : span.getTimedEvents()) {
        Map<String, String> wfAttrs = new HashMap<>(event.getAttributes().size());
        for (Map.Entry<String, AttributeValue> attr : event.getAttributes().entrySet()) {
          wfAttrs.put(attr.getKey(), attrToString(attr.getValue()));
        }
        spanLogs.add(new SpanLog(span.getStartEpochNanos() / 1000000, wfAttrs));
      }
      try {
        sender.sendSpan(
            span.getName(),
            span.getStartEpochNanos() / 1000000,
            (span.getEndEpochNanos() - span.getStartEpochNanos()) / 1000000,
            null, // TODO: Populate source
            makeUUID(span.getTraceId().toLowerBase16()),
            makeUUID(span.getSpanId().toLowerBase16()),
            Collections.singletonList(makeUUID(span.getParentSpanId().toLowerBase16())),
            null, // TODO: Populate followsFrom
            attrsToTags(span.getAttributes()),
            spanLogs);
      } catch (IOException e) {
        return ResultCode.FAILED_RETRYABLE;
      }
    }
    return ResultCode.SUCCESS;
  }

  @Override
  public void shutdown() {
    try {
      sender.flush();
      sender.close();
    } catch (IOException e) {
      logger.log(Level.WARNING, "Error closing Wavefront sender", e);
    }
  }

  public static class Builder {
    private WavefrontDirectIngestionClient.Builder directBuilder;

    private WavefrontProxyClient.Builder proxyBuilder;

    public static Builder newBuilder() {
      return new Builder();
    }

    public ProxyClientBuilder proxyClient(String host) {
      return new ProxyClientBuilder(host);
    }

    public DirectClientBuilder directClient(String wavefrontURL, String token) {
      return new DirectClientBuilder(wavefrontURL, token);
    }
  }

  public static class ProxyClientBuilder {
    private WavefrontProxyClient.Builder wfBuilder;

    private ProxyClientBuilder(String host) {
      wfBuilder = new WavefrontProxyClient.Builder(host);
    }

    public ProxyClientBuilder metricsPort(int metricsPort) {
      wfBuilder = wfBuilder.metricsPort(metricsPort);
      return this;
    }

    public ProxyClientBuilder distributionPort(int distributionPort) {
      wfBuilder = wfBuilder.distributionPort(distributionPort);
      return this;
    }

    public ProxyClientBuilder tracingPort(int tracingPort) {
      wfBuilder = wfBuilder.tracingPort(tracingPort);
      return this;
    }

    public ProxyClientBuilder socketFactory(SocketFactory socketFactory) {
      wfBuilder = wfBuilder.socketFactory(socketFactory);
      return this;
    }

    public ProxyClientBuilder flushIntervalSeconds(int flushIntervalSeconds) {
      wfBuilder = wfBuilder.flushIntervalSeconds(flushIntervalSeconds);
      return this;
    }

    public WavefrontSpanExporter build() {
      return new WavefrontSpanExporter(wfBuilder.build());
    }
  }

  public static class DirectClientBuilder {
    private WavefrontDirectIngestionClient.Builder wfBuilder;

    private DirectClientBuilder(String wavefrontURL, String token) {
      wfBuilder = new WavefrontDirectIngestionClient.Builder(wavefrontURL, token);
    }

    public DirectClientBuilder maxQueueSize(int maxQueueSize) {
      wfBuilder = wfBuilder.maxQueueSize(maxQueueSize);
      return this;
    }

    public DirectClientBuilder batchSize(int batchSize) {
      wfBuilder = wfBuilder.batchSize(batchSize);
      return this;
    }

    public DirectClientBuilder flushIntervalSeconds(int flushIntervalSeconds) {
      wfBuilder = wfBuilder.flushIntervalSeconds(flushIntervalSeconds);
      return this;
    }

    public DirectClientBuilder messageSizeBytes(int bytes) {
      wfBuilder = wfBuilder.messageSizeBytes(bytes);
      return this;
    }

    public WavefrontSpanExporter build() {
      return new WavefrontSpanExporter(wfBuilder.build());
    }
  }
}
