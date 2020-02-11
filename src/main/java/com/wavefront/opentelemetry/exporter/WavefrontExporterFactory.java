package com.wavefront.opentelemetry.exporter;

import io.opentelemetry.auto.exportersupport.ConfigProvider;
import io.opentelemetry.auto.exportersupport.ExporterFactory;
import io.opentelemetry.sdk.trace.export.SpanExporter;

public class WavefrontExporterFactory implements ExporterFactory {
  private final String PROXY = "wavefront.proxy";
  private final String WAVEFRONT_URL = "wavefront.url";
  private final String TRACEPORT = "wavefront.traceport";
  private final String METRICSPORT = "wavefront.metricsport";
  private final String FLUSH_INTERVAL = "wavefront.flushinterval";
  private final String TOKEN = "wavefront.token";

  @Override
  public SpanExporter fromConfig(ConfigProvider config) {
    WavefrontSpanExporter.Builder b = WavefrontSpanExporter.Builder.newBuilder();
    String proxy = config.getString(PROXY, null);
    String url = config.getString(WAVEFRONT_URL, null);
    if (proxy != null) {
      if (url != null) {
        throw new IllegalArgumentException(
            "Settings " + PROXY + " and " + WAVEFRONT_URL + " are mutualy exclusive");
      }
      WavefrontSpanExporter.ProxyClientBuilder pb = b.proxyClient(proxy);
      pb.metricsPort(config.getInt(METRICSPORT, 2878));
      pb.tracingPort(config.getInt(TRACEPORT, 30000));
      pb.flushIntervalSeconds(config.getInt(FLUSH_INTERVAL, 5));
      return pb.build();
    } else if (url != null) {
      String token = config.getString(TOKEN, null);
      if (token == null) {
        throw new IllegalArgumentException(
            "Setting " + TOKEN + " must be specified for direct connections");
      }
      WavefrontSpanExporter.DirectClientBuilder db = b.directClient(url, token);
      db.flushIntervalSeconds(config.getInt(FLUSH_INTERVAL, 5));
      return db.build();
    } else {
      throw new IllegalArgumentException(
          "Either " + PROXY + " or " + WAVEFRONT_URL + " need to be specified");
    }
  }
}
