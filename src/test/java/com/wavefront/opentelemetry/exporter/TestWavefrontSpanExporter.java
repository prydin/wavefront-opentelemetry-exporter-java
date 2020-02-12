package com.wavefront.opentelemetry.exporter;

import static org.junit.Assert.assertEquals;

import io.opentelemetry.auto.exportersupport.ConfigProvider;
import io.opentelemetry.sdk.trace.SpanData;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import io.opentelemetry.trace.Span;
import io.opentelemetry.trace.SpanId;
import io.opentelemetry.trace.Status;
import io.opentelemetry.trace.TraceId;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

public class TestWavefrontSpanExporter {
  private TestConfig createDefaultConfig() {
    final int tracingPort = getFreePort(50000);
    final int metricPort = getFreePort(2878);
    final TestConfig config = new TestConfig("exporter");
    config.put("wavefront.proxy", "localhost");
    config.put("wavefront.metricport", Integer.toString(metricPort));
    config.put("wavefront.traceport", Integer.toString(tracingPort));
    config.put("application", "test-application");
    config.put("service", "test-service");
    return config;
  }

  private SpanExporter createDefault(final TestConfig config) {
    final WavefrontExporterFactory f = new WavefrontExporterFactory();
    return f.fromConfig(config);
  }

  private SpanExporter createDefault() {
    return createDefault(createDefaultConfig());
  }

  private List<SpanData> createTestSpans() {
    final List<SpanData> list = new ArrayList<>();
    final TraceId tid = TraceId.fromLowerBase16("0123456789abcdef0000000000000000", 0);
    final SpanId sid1 = SpanId.fromLowerBase16("0000000000000000", 0);
    final SpanId sid2 = SpanId.fromLowerBase16("1111111111111111", 0);
    final long nowInNanos = System.currentTimeMillis() * 1000000;
    list.add(
        SpanData.newBuilder()
            .setName("client.span")
            .setKind(Span.Kind.CLIENT)
            .setTraceId(tid)
            .setSpanId(sid1)
            .setStartEpochNanos(nowInNanos)
            .setEndEpochNanos(nowInNanos + 1000000)
            .setStatus(Status.OK)
            .build());
    list.add(
        SpanData.newBuilder()
            .setName("server.span")
            .setKind(Span.Kind.SERVER)
            .setTraceId(tid)
            .setSpanId(sid2)
            .setStartEpochNanos(nowInNanos + 2000000)
            .setEndEpochNanos(nowInNanos + 3000000)
            .setStatus(Status.OK)
            .build());
    return list;
  }

  private int getFreePort(final int start) {
    int port = start;
    for (; ; ++port) {
      try (final ServerSocket ss = new ServerSocket(port)) {
        return port;
      } catch (final IOException e) {
        System.out.println(e);
      }
    }
  }

  @Test
  public void testHex() {
    assertEquals(2, WavefrontSpanExporter.parseHex("2"));
    assertEquals(0xffffL, WavefrontSpanExporter.parseHex("ffff"));
    assertEquals(-1L, WavefrontSpanExporter.parseHex("ffffffffffffffff"));
    assertEquals(-2L, WavefrontSpanExporter.parseHex("fffffffffffffffe"));
    assertEquals(0x7fffffffffffffffL, WavefrontSpanExporter.parseHex("7fffffffffffffff"));
  }

  @Test
  public void testCreate() {
    final SpanExporter e = createDefault();
    assertEquals(
        "com.wavefront.opentelemetry.exporter.WavefrontSpanExporter", e.getClass().getName());
  }

  @Test
  public void testReportSpan() throws IOException, InterruptedException {
    final TestConfig config = createDefaultConfig();
    final SpanExporter e = createDefault(config);
    final MockServer ms = new MockServer(config.getInt("wavefront.traceport", 0), 2);
    final Thread t = new Thread(ms);
    t.start();
    e.export(createTestSpans());
    for (int i = 0; i < 2; ++i) {
      final String s = ms.poll(10000);
      System.out.println(s);
    }
  }

  @Test
  public void testReportSpanToProxy() throws IOException, InterruptedException {
    final TestConfig config = createDefaultConfig();
    config.map.put("exporter.wavefront.traceport", "50000");
    final SpanExporter e = createDefault(config);
    e.export(createTestSpans());
  }

  private static class MockServer implements Runnable {
    private final ServerSocket socket;

    private final int expectedMessages;

    private final BlockingQueue<String> queue;

    public MockServer(final int port, final int expectedMessages) throws IOException {
      socket = new ServerSocket(port);
      this.expectedMessages = expectedMessages;
      queue = new ArrayBlockingQueue<>(expectedMessages);
    }

    @Override
    public void run() {
      final Timer t = new Timer();
      t.schedule(
          new TimerTask() {
            @Override
            public void run() {
              try {
                socket.close();
              } catch (final IOException e) {
              }
            }
          },
          10000);
      try (final Socket conn = socket.accept()) {
        conn.setSoTimeout(10000);
        final BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream()));
        for (int i = 0; i < expectedMessages; ++i) {
          final String line = br.readLine();
          queue.put(line);
        }
      } catch (final IOException e) {
        e.printStackTrace();
      } catch (final InterruptedException e) {
        // Just quit
      }
    }

    public String poll(final long timeout) throws InterruptedException {
      return queue.poll(timeout, TimeUnit.MILLISECONDS);
    }
  }

  public class TestConfig implements ConfigProvider {
    private final String prefix;

    private final Map<String, String> map = new HashMap<>();

    public TestConfig(final String prefix) {
      this.prefix = prefix;
    }

    private void put(final String k, final String v) {
      map.put(prefix + "." + k, v);
    }

    // @Override
    @Override
    public String getString(final String key, final String defaultValue) {
      final String s = map.get(prefix + "." + key);
      return s != null ? s : defaultValue;
    }

    // @Override
    @Override
    public int getInt(final String key, final int defaultValue) {
      final String s = map.get(prefix + "." + key);
      if (s == null) {
        return defaultValue;
      }
      return Integer.parseInt(s);
    }

    // @Override
    @Override
    public long getLong(final String key, final int defaultValue) {
      final String s = map.get(prefix + "." + key);
      if (s == null) {
        return defaultValue;
      }
      return Long.parseLong(s);
    }

    // @Override
    @Override
    public boolean getBoolean(final String key, final boolean defaultValue) {
      final String s = map.get(prefix + "." + key);
      if (s == null) {
        return defaultValue;
      }
      return Boolean.parseBoolean(s);
    }

    // @Override
    @Override
    public double getDouble(final String key, final double defaultValue) {
      final String s = map.get(prefix + "." + key);
      if (s == null) {
        return defaultValue;
      }
      return Double.parseDouble(s);
    }
  }
}
