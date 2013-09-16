package com.continuuity.weave.internal.appmaster;

import com.continuuity.weave.api.ResourceReport;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.http.DefaultHttpRequest;
import org.jboss.netty.handler.codec.http.HttpChunkAggregator;
import org.jboss.netty.handler.codec.http.HttpClientCodec;
import org.jboss.netty.handler.codec.http.HttpContentDecompressor;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.jboss.netty.util.CharsetUtil;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.ProtocolException;
import java.net.URL;
import java.util.concurrent.Executors;

/**
 * Client to get {@link ResourceReport} from the application master.
 */
public class ResourceReportClient {
  private final ResourceReportAdapter reportAdapter;
  private final String resourcesUrl;
  private static final String REPORT_PATH = "/resources";

  public ResourceReportClient(String host, int port) {
    resourcesUrl = "http://" + host + ":" + port + "/resources";
    this.reportAdapter = ResourceReportAdapter.create();
  }

  public ResourceReport get() {
    ResourceReport report = null;
    try {
      URL url = new URL(resourcesUrl);
      HttpURLConnection con = (HttpURLConnection) url.openConnection();
      con.setRequestMethod("GET");

      //add request header
      //con.setRequestProperty("User-Agent", USER_AGENT);

      int responseCode = con.getResponseCode();
      System.out.println("Response Code : " + responseCode);

      Reader reader = new InputStreamReader(con.getInputStream());
      report = reportAdapter.fromJson(reader);
      reader.close();
      con.disconnect();
    } catch (IOException e) {

    }
    return report;
  }
}
