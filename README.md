# OpenTelemetry Exporter for Wavefront (Experimental)

## Disclaimer
The OpenTelemetry project is still under active development and not yet officially released. APIs may change without notice and may break this exporter.

## How to Use
TODO: Need to put this in a repo before we can write instructions on how to link to it.

## OpenTelemetry Java Auto Instrumentation Compatibility
This exporter is intended to be compatible with the Open Telemetry Java Auto Instrumentation. The specification for this is still under development.

These are the attributes currently supported by this exporter.


Setting | System Property | Environment variable 
--- | --- | --- 
Proxy address (mutually exclusive with backend URL) | ota.exporter.wavefront.proxy | OTA_EXPORTER_WAVEFRONT_PROXY 
Proxy metrics port (mutually exclusive with backend URL) | ota.exporter.wavefront.metricsport | OTA_EXPORTER_WAVEFRONT_METRICSPORT
Proxy tracing port (mutually exclusive with backend URL) | ota.exporter.wavefront.traceport | OTA_EXPORTER_WAVEFRONT_TRACEPORT
Direct ingestion Wavefront URL (mutually exclusive with proxy settings)| ota.exporter.wavefront.url | OTA_EXPORTER_WAVEFRONT_URL 
Direct ingestion API token (mutually exclusive with proxy settings) | ota.exporter.wavefront.token | OTA_EXPOERTER_WAVEFRONT_TOKEN
Flush interval (in seconds) | ota.exporter.wavefront.flushinterval | OTA_EXPORTER_WAVEFRONT_FLUSHINTERVAL 
Service tag | ota.exporter.service | OTA_EXPORTER_SERVICE
Application tag | ota.exporter.application | OTA_EXPORTER_APPLICATION
