package com.wavefront.opentelemetry.exporter;

import com.google.common.annotations.VisibleForTesting;
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
  private static final int NUM_STD_TAGS = 3;
  private static final Logger logger =
      Logger.getLogger(WavefrontSpanExporter.class.getCanonicalName());
  private final WavefrontSender sender;
  private final String host;
  private final String application;
  private final String service;

  protected WavefrontSpanExporter(
      final WavefrontSender sender,
      final String host,
      final String application,
      final String service) {
    this.sender = sender;
    this.application = application;
    this.service = service;
    this.host = host;
  }

  @VisibleForTesting
  protected static long parseHex(final String s) {
    final int l = s.length();
    if (l > 16) {
      throw new NumberFormatException("Too many characters in " + s);
    }
    if (l == 16 && s.charAt(0) > '7') {
      return (Long.parseLong(s.substring(0, 8), 16) << 32) | (Long.parseLong(s.substring(8), 16));
    }
    return Long.parseLong(s, 16);
  }

  private static UUID makeUUID(final String s) {
    if (s.length() <= 16) {
      return new UUID(0, parseHex(s));
    }
    return new UUID(parseHex(s.substring(0, 16)), parseHex(s.substring(16)));
  }

  private static String attrToString(final AttributeValue attr) {
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

  private List<Pair<String, String>> extractTags(final SpanData span) {
    final Map<String, AttributeValue> attrs = span.getAttributes();
    final List<Pair<String, String>> tags = new ArrayList<>(attrs.size() + NUM_STD_TAGS);
    tags.add(new Pair<>("application", application));
    tags.add(new Pair<>("service", service));
    final String instLibName = span.getInstrumentationLibraryInfo().name();
    if (instLibName != null) {
      tags.add(new Pair<>("instrumentaton.name", instLibName));
    }
    final String instLibVer = span.getInstrumentationLibraryInfo().version();
    if (instLibVer != null) {
      tags.add(new Pair<>("instrumentation.version", instLibVer));
    }

    for (final Map.Entry<String, AttributeValue> attr : attrs.entrySet()) {
      tags.add(new Pair<>(attr.getKey(), attrToString(attr.getValue())));
    }
    return tags;
  }

  @Override
  public ResultCode export(final List<SpanData> spans) {
    for (final SpanData span : spans) {
      logger.log(Level.FINE, "SPAN: " + span.getName());
      final List<SpanLog> spanLogs = new ArrayList<>(spans.size());
      for (final SpanData.TimedEvent event : span.getTimedEvents()) {
        final Map<String, String> wfAttrs = new HashMap<>(event.getAttributes().size());
        for (final Map.Entry<String, AttributeValue> attr : event.getAttributes().entrySet()) {
          wfAttrs.put(attr.getKey(), attrToString(attr.getValue()));
        }
        spanLogs.add(new SpanLog(span.getStartEpochNanos() / 1000000, wfAttrs));
      }
      try {
        sender.sendSpan(
            span.getName(),
            span.getStartEpochNanos() / 1000000,
            (span.getEndEpochNanos() - span.getStartEpochNanos()) / 1000000,
            host,
            makeUUID(span.getTraceId().toLowerBase16()),
            makeUUID(span.getSpanId().toLowerBase16()),
            Collections.singletonList(makeUUID(span.getParentSpanId().toLowerBase16())),
            null, // TODO: Populate followsFrom
            extractTags(span),
            spanLogs);
      } catch (final IOException e) {
        logger.log(Level.WARNING, "Error while sending span", e);
        return ResultCode.FAILED_RETRYABLE;
      } catch (final Throwable t) {
        logger.log(Level.WARNING, "Error while sending span", t);
      }
    }
    return ResultCode.SUCCESS;
  }

  @Override
  public void shutdown() {
    try {
      sender.flush();
      sender.close();
    } catch (final IOException e) {
      logger.log(Level.WARNING, "Error closing Wavefront sender", e);
    }
  }

  /** Builds and configures a {@link WavefrontSpanExporter} */
  public static class Builder {
    private String host = null;
    private String application = "(unknown application)";
    private String service = "(unknown service)";
    private WavefrontDirectIngestionClient.Builder directBuilder;
    private WavefrontProxyClient.Builder proxyBuilder;

    /**
     * Creates a new {@link Builder}
     *
     * @return
     */
    public static Builder newBuilder() {
      return new Builder();
    }

    /**
     * Creates a builder for a proxy client
     *
     * @param host The name or IP address of the proxy host.
     * @return
     */
    public ProxyClientBuilder proxyClient(final String host) {
      return new ProxyClientBuilder(host, this);
    }

    /**
     * Creates a builder for a direct ingestion client
     *
     * @param wavefrontURL The full URL of the Wavefront endpoint
     * @param token The API token to use for authentication
     * @return
     */
    public DirectClientBuilder directClient(final String wavefrontURL, final String token) {
      return new DirectClientBuilder(wavefrontURL, token, this);
    }

    /**
     * Sets the service name to be used for the "service" tag on spans.
     *
     * @param service
     * @return
     */
    public Builder service(final String service) {
      this.service = service;
      return this;
    }

    /**
     * Sets the application name to be used for the "application" tag on spans.
     *
     * @param application The application name
     * @return
     */
    public Builder application(final String application) {
      this.application = application;
      return this;
    }

    /**
     * Sets the originating host name for spans. If not specified, the exporter will automatically
     * infer the host name.
     *
     * @param host The host name
     * @return
     */
    public Builder host(final String host) {
      this.host = host;
      return this;
    }
  }

  public static class ProxyClientBuilder {
    private final Builder parent;
    private WavefrontProxyClient.Builder wfBuilder;

    private ProxyClientBuilder(final String host, final Builder parent) {
      wfBuilder = new WavefrontProxyClient.Builder(host);
      this.parent = parent;
    }

    /**
     * Specifies the metrics port. Default is 2878
     *
     * @param metricsPort The metrics port
     * @return
     */
    public ProxyClientBuilder metricsPort(final int metricsPort) {
      wfBuilder = wfBuilder.metricsPort(metricsPort);
      return this;
    }

    /**
     * Specifies the distributions (histogram) port. Default is 2878.
     *
     * @param distributionPort The distribution port
     * @return
     */
    public ProxyClientBuilder distributionPort(final int distributionPort) {
      wfBuilder = wfBuilder.distributionPort(distributionPort);
      return this;
    }

    /**
     * Sets the tracing (span) port. The default is 30000
     *
     * @param tracingPort The Tracing port
     * @return
     */
    public ProxyClientBuilder tracingPort(final int tracingPort) {
      wfBuilder = wfBuilder.tracingPort(tracingPort);
      return this;
    }

    /**
     * Specifies a custom {@link SocketFactory}
     *
     * @param socketFactory The custom {@link SocketFactory}
     * @return
     */
    public ProxyClientBuilder socketFactory(final SocketFactory socketFactory) {
      wfBuilder = wfBuilder.socketFactory(socketFactory);
      return this;
    }

    /**
     * Specifies the flush interval (in seconds). This specifies how often the exporter tries to
     * flush its internal buffers and send data to the backend. The default is 5s.
     *
     * @param flushIntervalSeconds The flush interval in seconds
     * @return
     */
    public ProxyClientBuilder flushIntervalSeconds(final int flushIntervalSeconds) {
      wfBuilder = wfBuilder.flushIntervalSeconds(flushIntervalSeconds);
      return this;
    }

    /**
     * Builds a new {@link WavefrontSpanExporter}
     *
     * @return
     */
    public WavefrontSpanExporter build() {
      return new WavefrontSpanExporter(
          wfBuilder.build(), parent.host, parent.application, parent.service);
    }
  }

  public static class DirectClientBuilder {
    private final Builder parent;
    private WavefrontDirectIngestionClient.Builder wfBuilder;

    private DirectClientBuilder(
        final String wavefrontURL, final String token, final Builder parent) {
      wfBuilder = new WavefrontDirectIngestionClient.Builder(wavefrontURL, token);
      this.parent = parent;
    }

    /**
     * Sets the maximum queue size
     *
     * @param maxQueueSize The maximum queue size
     * @return
     */
    public DirectClientBuilder maxQueueSize(final int maxQueueSize) {
      wfBuilder = wfBuilder.maxQueueSize(maxQueueSize);
      return this;
    }

    /**
     * Sets the maximum batch size
     *
     * @param batchSize The maximum batch size
     * @return
     */
    public DirectClientBuilder batchSize(final int batchSize) {
      wfBuilder = wfBuilder.batchSize(batchSize);
      return this;
    }

    /**
     * Specifies the flush interval (in seconds). This specifies how often the exporter tries to
     * flush its internal buffers and send data to the backend. The default is 5s.
     *
     * @param flushIntervalSeconds The flush interval in seconds
     * @return
     */
    public DirectClientBuilder flushIntervalSeconds(final int flushIntervalSeconds) {
      wfBuilder = wfBuilder.flushIntervalSeconds(flushIntervalSeconds);
      return this;
    }

    /**
     * Sets the maximum message size in bytes that will be transmitted to the backend.
     *
     * @param bytes The maximum message in bytes
     * @return
     */
    public DirectClientBuilder messageSizeBytes(final int bytes) {
      wfBuilder = wfBuilder.messageSizeBytes(bytes);
      return this;
    }

    public WavefrontSpanExporter build() {
      return new WavefrontSpanExporter(
          wfBuilder.build(), parent.host, parent.application, parent.service);
    }
  }
}
