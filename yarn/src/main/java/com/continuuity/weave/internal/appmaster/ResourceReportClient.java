package com.continuuity.weave.internal.appmaster;

import com.continuuity.weave.api.ResourceReport;
import com.continuuity.weave.internal.json.ResourceReportAdapter;
import com.google.common.base.Charsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * Client to get {@link ResourceReport} from the application master.
 */
public class ResourceReportClient {
  private static final Logger LOG = LoggerFactory.getLogger(ResourceReportClient.class);
  private static final String REPORT_PATH = "/resources";

  private final ResourceReportAdapter reportAdapter;
  private final String resourcesUrl;

  public ResourceReportClient(String host, int port) {
    resourcesUrl = "http://" + host + ":" + port + REPORT_PATH;
    this.reportAdapter = ResourceReportAdapter.create();
  }

  public ResourceReport get() {
    ResourceReport report = null;
    Reader reader = null;
    HttpURLConnection conn = null;
    try {
      URL url = new URL(resourcesUrl);
      conn = (HttpURLConnection) url.openConnection();
      conn.setRequestMethod("GET");

      reader = new InputStreamReader(conn.getInputStream(), Charsets.UTF_8);
      report = reportAdapter.fromJson(reader);
    } catch (IOException e) {
      LOG.error("Exception getting resource report from " + resourcesUrl, e);
    } finally {
      if (reader != null) {
        try {
          reader.close();
        } catch (IOException e) {
          LOG.error("Exception closing reader", e);
        }
      }
      if (conn != null) {
        conn.disconnect();
      }
    }
    return report;
  }
}
