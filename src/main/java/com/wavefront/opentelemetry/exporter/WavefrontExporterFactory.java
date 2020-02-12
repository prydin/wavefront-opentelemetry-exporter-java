package com.wavefront.opentelemetry.exporter;

import io.opentelemetry.auto.exportersupport.ConfigProvider;
import io.opentelemetry.auto.exportersupport.ExporterFactory;
import io.opentelemetry.sdk.trace.export.SpanExporter;

/**
 * Implements an {@link ExporterFactory} that is called from the Java Auto Instrumenter.
 * Applications that are programmatically constructing a {@link WavefrontSpanExporter} should use
 * the builder instead.
 */
public class WavefrontExporterFactory implements ExporterFactory {
  private final String PROXY = "wavefront.proxy";
  private final String WAVEFRONT_URL = "wavefront.url";
  private final String TRACEPORT = "wavefront.traceport";
  private final String METRICSPORT = "wavefront.metricsport";
  private final String FLUSH_INTERVAL = "wavefront.flushinterval";
  private final String TOKEN = "wavefront.token";
  private final String HOST = "wavefront.host";
  private final String APPLICAITION = "application";
  private final String SERVICE = "service";

  /**
   * Called from the Java Auto Instrumenter to create a new {@link WavefrontSpanExporter}
   *
   * @param config The configuration to use
   * @return
   */
  @Override
  public SpanExporter fromConfig(final ConfigProvider config) {
    WavefrontSpanExporter.Builder b = WavefrontSpanExporter.Builder.newBuilder();
    b =
        b.application(config.getString(APPLICAITION, "(unknown application)"))
            .service(config.getString(SERVICE, "(unknown service)"))
            .host(config.getString(HOST, null));

    final String proxy = config.getString(PROXY, null);
    final String url = config.getString(WAVEFRONT_URL, null);
    if (proxy != null) {
      if (url != null) {
        throw new IllegalArgumentException(
            "Settings " + PROXY + " and " + WAVEFRONT_URL + " are mutually exclusive");
      }
      final WavefrontSpanExporter.ProxyClientBuilder pb = b.proxyClient(proxy);
      pb.metricsPort(config.getInt(METRICSPORT, 2878));
      pb.tracingPort(config.getInt(TRACEPORT, 30000));
      pb.flushIntervalSeconds(config.getInt(FLUSH_INTERVAL, 5));
      return pb.build();
    } else if (url != null) {
      final String token = config.getString(TOKEN, null);
      if (token == null) {
        throw new IllegalArgumentException(
            "Setting " + TOKEN + " must be specified for direct connections");
      }
      final WavefrontSpanExporter.DirectClientBuilder db = b.directClient(url, token);
      db.flushIntervalSeconds(config.getInt(FLUSH_INTERVAL, 5));
      return db.build();
    } else {
      throw new IllegalArgumentException(
          "Either " + PROXY + " or " + WAVEFRONT_URL + " need to be specified");
    }
  }
}
