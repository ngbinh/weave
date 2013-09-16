package com.continuuity.weave.internal.appmaster;

import com.continuuity.weave.api.ResourceReport;
import com.continuuity.weave.api.WeaveRunResources;
import com.google.common.util.concurrent.AbstractIdleService;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpChunkAggregator;
import org.jboss.netty.handler.codec.http.HttpContentCompressor;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.jboss.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Webservice that the Application Master will register back to the resource manager
 * for clients to track application progress.  Currently used purely for getting a
 * breakdown of resource usage as a {@link com.continuuity.weave.api.ResourceReport}.
 */
public final class TrackerService extends AbstractIdleService {

  private static final Logger LOG  = LoggerFactory.getLogger(TrackerService.class);

  private ServerBootstrap bootstrap;
  private InetSocketAddress bindAddress;
  private final ChannelGroup channelGroup;
  private final ResourceReport resourceReport;

  private static final int CLOSE_CHANNEL_TIMEOUT = 5;
  static final int MAX_INPUT_SIZE = 100 * 1024 * 1024;

  /**
   * Initialize Service
   */
  public TrackerService(int port, ResourceReport resourceReport) {
    this.bindAddress = new InetSocketAddress(port);
    this.channelGroup = new DefaultChannelGroup("appMasterTracker");
    this.resourceReport = resourceReport;
  }

  @Override
  protected void startUp() throws Exception {
    ChannelFactory factory = new NioServerSocketChannelFactory(
      Executors.newCachedThreadPool(), Executors.newCachedThreadPool());

    bootstrap = new ServerBootstrap(factory);

    bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
      public ChannelPipeline getPipeline() {
        ChannelPipeline pipeline = Channels.pipeline();

        pipeline.addLast("decoder", new HttpRequestDecoder());
        pipeline.addLast("aggregator", new HttpChunkAggregator(MAX_INPUT_SIZE));
        pipeline.addLast("encoder", new HttpResponseEncoder());
        pipeline.addLast("compressor", new HttpContentCompressor());
        pipeline.addLast("handler", new ReportHandler(resourceReport));

        return pipeline;
      }
    });

    channelGroup.add(bootstrap.bind(bindAddress));
  }

  @Override
  protected void shutDown() throws Exception {
    try {
      if (!channelGroup.close().await(CLOSE_CHANNEL_TIMEOUT, TimeUnit.SECONDS)) {
        LOG.warn("Timeout when closing all channels.");
      }
    } finally {
      bootstrap.releaseExternalResources();
    }
  }

  /**
   * Handler to return resources used by this application master, which will be available through
   * the host and port set when this application master registered itself to the resource manager.
   */
  public class ReportHandler extends SimpleChannelUpstreamHandler {
    private final ResourceReport report;
    private final ResourceReportAdapter reportAdapter;

    public ReportHandler(ResourceReport report) {
      this.report = report;
      this.reportAdapter = ResourceReportAdapter.create();
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
      HttpRequest request = (HttpRequest) e.getMessage();
      if (!isValid(request)) {
        write404(e);
        return;
      }

      writeResponse(e);
    }

    // only accepts GET on /resources for now
    private boolean isValid(HttpRequest request) {
      return (request.getMethod() == HttpMethod.GET) && "/resources".equals(request.getUri());
    }

    private void write404(MessageEvent e) {
      HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NOT_FOUND);
      ChannelFuture future = e.getChannel().write(response);
      future.addListener(ChannelFutureListener.CLOSE);
    }

    private void writeResponse(MessageEvent e) {
      HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
      response.setContent(ChannelBuffers.copiedBuffer(reportAdapter.toJson(report), CharsetUtil.UTF_8));
      response.setHeader(HttpHeaders.Names.CONTENT_TYPE, "text/plain; charset=UTF-8");
      ChannelFuture future = e.getChannel().write(response);
      future.addListener(ChannelFutureListener.CLOSE);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
      e.getChannel().close();
    }
  }
}

