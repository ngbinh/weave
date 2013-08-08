What is Weave ?
===============

Weave is a simple set of libraries for easily managing distributed applications through an abstraction layer built on Apache Hadoop&reg; YARN. 

Weave allows you to use YARN’s distributed capabilities with a programming model that's similar to running threads. Weave is **NOT** a replacement for YARN. Weave is a value-added framework that operates on top of YARN.

Why Do I Need Weave?
=====================
Weave dramatically simplifies and reduces development efforts, enabling you to quickly and easily manage 
distributed applications through its friendly abstraction layer built on YARN. YARN can be quite difficult to use and requires a big ramp up effort, especially since it's only built for MapReduce and it's normally meant for managing batch jobs. YARN, however, can be used as a generalized custom resource management tool that can run any type of job including batch jobs, real-time jobs, and long-running jobs. 

Unfortunately, YARN’s capabilities are too low level to allow you to quickly develop applications. YARN requires a great deal of boilerplate code even for simple applications, and its logging output does not become available until the application stops running. This becomes especially problematic when managing long-running jobs. These jobs never finish so you can't view their logs, which makes it very difficult to develop and debug them. Finally, YARN does not provide standard support for application lifecycle management, communications between containers and the Application Master, and handling application level errors. 

Weave allows you to manage your distributed applications with a much simpler programming model. You can quickly and easily build, test, run, and debug YARN applications, dramatically reducing your development efforts and simplifying the management of resources and jobs in your distributed applications.

Weave provides you with the following benefits:

  * A simplified API for specifying, running and managing applications
  * A simplified way to specify and manage the stages of the application lifecycle
  * A generic Application Master to better support simple applications
  * Simplified archive management
  * Log and metrics aggregation for your applications, with improved control over application logs, metrics and errors
  * A discovery service
  
Getting Started
=========
This section will help you understand how to run your apps on a YARN cluster using Weave.

Build the Weave Library
------------------
    $ git clone http://github.com/continuuity/weave.git
    $ cd weave
    $ mvn install

Quick Example
=========
Let's begin by building a basic EchoServer with Java. Traditionally, when you build a server as simple as this, you add logic within a Runnable implementation to run it in a Thread using an appropriate ExecutorService:

    public class EchoServer implements Runnable {
        private static Logger LOG = LoggerFactory.getLogger(EchoServer.class);
        private final ServerSocket serverSocket;
        
        public EchoServer() {
            ...
        }
    
        @Override
        public void run() {
            while ( isRunning() ) {
                Socket socket = serverSocket.accept();
                    ...
            }
        }
    }

The example above defines an implementation of Runnable that implements the run method. The EchoServer is now a Runnable that can be executed by an ExecutorService in a Thread:

    ...
    ExecutorService service = Executors.newFixedThreadPool(2);
    service.submit(new EchoServer());
    ...


Implement WeaveRunnable
------------------
This EchoServer model above is familiar, but what if you want to run your EchoServer on a YARN cluster? 

All you need to do is implement the WeaveRunnable interface, similar to how you would normally implement Runnable. In this model, the EchoServer implements WeaveRunnable, which in turn implements Runnable. This allows you to run a WeaveRunnable implementation within a Thread and also in a container on a YARN cluster:

    public class EchoServer implements WeaveRunnable {
        private static Logger LOG = LoggerFactory.getLogger(EchoServer.class);
        private final ServerSocket serverSocket;
    
        public EchoServer() {
            ...
        }
         
        @Override
        public void run() {
            while ( isRunning() ) {
                Socket socket = serverSocket.accept();
                ...
            }
        }
    }

Start the Weave Runner Service
------------------
In order to run EchoServer on the YARN cluster we must create a WeaveRunnerService, which is similar to ExecutorService. Then we specify the YARN cluster configuration and a connection string to a running instance of a Zookeeper service:

    WeaveRunnerService runnerService = new YarnWeaveRunnerService(
        new YarnConfiguration(), zkServer.getConnectionString());
    runnerService.startAndWait();

Start the Weave Controller and Add a Log Handler
------------------
Now that the WeaveRunnerService is initialized we can prepare to run the EchoServer on the YARN cluster by attaching a log handler that ensures that all logs generated by the EchoServer across all nodes in the cluster are centralized on the client. 

Note that you do not need to specify the archives that must be shipped to remote machines on the YARN cluster (where the container will run). This is all taken care of by Weave:

    WeaveController controller = runnerService.prepare(new EchoServer())
        .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out)))
        .start();

Add a Listener for State Transitions
------------------
Now that we have started, prepared, and launched an EchoServer on the YARN cluster, we can attach a listener that allows us to observe state transitions in our application:

    controller.addListener(new ListenerAdapter() {
        @Override
        public void running() {
            LOG.info('Echo Server Started');
        }
    }

Stop WeaveRunnable
------------------
To stop the running EchoServer, use the controller object reference returned during the start of the application. This shuts down the Application Master and all of the configured containers:

    controller.stop().get();

Advanced Weave Examples
=========
This section discusses some advanced Weave features.

Discovery Service
------------------
The EchoServer is useful only if it's discoverable. Clients that want to access the server running in a cluster must be able to connect to the service and talk to it. Weave helps you accomplish this important task by exposing a discovery service that allows your running Weave application to announce itself on the cluster, making it possible for the client to discover and connect to it. 

Let's see how we can add this capability to the EchoServer. The EchoServer starts on a port available on the machine on which it started, then announces its presence via the Weave discovery service API. In this example, the class extends AbstractWeaveRunnable, which implements WeaveRunnable, which implements Runnable:

WeaveRunnable with Discovery Announce
--------------
    public class EchoServer extends AbstractWeaveRunnable {
        private static final Logger LOG = LoggerFactory.getLogger(EchoServer.class);
         
        @Override
        public void initialize(WeaveContext context) {
            super.initialize(context);
            ...
            try {
                serverSocket = new ServerSocket(0); // start on any available port
                context.announce("echo", serverSocket.getLocalPort());
            } catch (IOException e) {
                throw Throwables.propogate(e);
            }
        }
     
        @Override
        public void run() {
            ...
        }
    }

During the initialization phase of the container, WeaveContext used the port on which EchoServer was started to announce the EchoServer’s presence. This allows clients to discover the echo service using an iterator:

    ...
    WeaveController controller = ... 
    ...
    Iterable<Discoverable> echoServices = controller.discoverService("echo");
    ...
    for(Discoverable discoverable : echoServices) {
        Socket socket = new Socket(discoverable.getSocketAddress().getAddress(),
                                   discoverable.getSocketAddress().getPort());
    ...
    }

Logging with SLF4J
------------------
In the earlier examples a log handler was attached when we were preparing to run an implementation of WeaveRunnable. It is used for collecting all logs emitted by the containers, and these logs are returned to the client. This means that you don't have to run your IDE on the YARN cluster, just use a standard SLF4J logger to log messages within the container. The logs are hijacked and sent through a Kafka broker to the client. (A Kafka broker is started within the Application Master when each application is launched.)

    public class EchoServer extends AbstractWeaveRunnable {
        private static final Logger LOG = LoggerFactory.getLogger(EchoServer.class);
        ...
        @Override
        public void run() {
            ...
            LOG.info('New client accepted');
            ...
        }
        ...
    }

Resource Specification
------------------
When you prepare an implementation of WeaveRunnable to run on a YARN cluster, you must specify the resources used to run the container.

Specify Resource Constraints for a Container
--------------
You can specify assets like the number of cores to be used, the amount of memory, and the number of instances. Internally Weave uses cgroups to limit the amount of system resources used by the container:

    WeaveController controller = runnerService.prepare(new EchoServer(port),
        ResourceSpecification().Builder().with()
            .setCores(1)
            .setMemory(1, ResourceSpecification.SizeUnit.GIGA)
            .setInstances(2).build())
            .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out)))
            .start();

Archive Management
------------------
In order to run in a container on a YARN cluster, all of the necessary JAR files must be marshaled to the node on which the container is running. This is all handled internally by Weave, but the Weave APIs also allow you to specify additional files to be marshaled to the node where the container is running.

Applications
------------------
A WeaveApplication is a collection of distributed WeaveRunnable instances working together. For example, suppose you have a web application that you would like to deploy on a cluster running YARN. You will need instances of a Jetty server and all associated files to serve the application:

    public class WebApplication implements WeaveApplication {
        @Override
        public WeaveSpecification configure() {
            return WeaveSpecification().Builder.with()
                .setName("My Web Application")
                .withRunnables()
                .add(new JettyWebServer())
                .withLocalFiles()
                .add("html-pages.tgz", pages, true)
                .apply()
                .add(new LogsCollector())
                .anyOrder()
                .build();
        }
    }

Once you define an application in Weave you run it the same way you run WeaveRunnable. You might notice from the example above that Weave applications support the order in which WeaveRunnable instances are started on the cluster. 

Ordering
--------------
The example above specifies no order via the anyOrder() method, so all the WeaveRunnables can start concurrently. However, you can specify the order:

    public class WebApplication implements WeaveApplication {
        @Override
        WeaveSpecification configure() {
            return WeaveSpecification().Builder.with()
                .setName("My Web Application")
                .withRunnables()
                .add("jetty", new JettyWebServer())
                .withLocalFiles()
                .add("html-pages.tgz", pages, true)
                .apply()
                .add("log", new LogsCollector())
                .order()
                .first("log")
                .next("jetty")
                .build();
        }
    }

Code Samples
=========
Here are some automated Weave tests that we use at Continuuity. There are additional tests in the [yarn](https://github.com/continuuity/weave/blob/master/yarn/src/test/java/com/continuuity/weave/yarn).

* [EchoServer](https://github.com/continuuity/weave/blob/master/yarn/src/test/java/com/continuuity/weave/yarn/EchoServer.java) 
* [EchoServerTestRun](https://github.com/continuuity/weave/blob/master/yarn/src/test/java/com/continuuity/weave/yarn/EchoServerTestRun.java) 

APIs
=========
* [Weave Doc Index](http://continuuity.github.io/weave/apidocs/index.html) 
* [Weave API](http://continuuity.github.io/weave/apidocs/com/continuuity/weave/api/package-summary.html) 
* [Weave Yarn](http://continuuity.github.io/weave/apidocs/com/continuuity/weave/yarn/package-summary.html) 
* [Weave Common](http://continuuity.github.io/weave/apidocs/com/continuuity/weave/common/package-summary.html) 
* [Weave Discovery](http://continuuity.github.io/weave/apidocs/com/continuuity/weave/discovery/package-summary.html) 
* [Weave Zookeeper](http://continuuity.github.io/weave/apidocs/com/continuuity/weave/zookeeper/package-summary.html) 

Community
=========
How to Contribute
------------------
Interested in improving Weave? We have a simple pull-based development model with a consensus-building phase, similar to Apache's voting process. If you’d like to help make Weave better by adding new features, enhancing existing features, or fixing bugs, here's how to do it:

  * Fork weave into your own GitHub repository
  * Create a topic branch with an appropriate name
  * Work on the code to your heart's content
  * Once you’re satisfied, create a pull request from your GitHub repo (it’s helpful if you fill in all of the description fields)
  * After we review and accept your request we’ll commit your code to the continuuity/weave repo
  * Thanks!

Groups
------
* User Group: [weave-user](https://groups.google.com/d/forum/weave-user)

License
=======
Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
