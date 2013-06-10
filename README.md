What is Weave ?
===============

Weave is a simple set of libraries that allows you to easily manage distributed applications through an abstraction layer 
built on Apache YARN. Weave allows you to use YARN’s distributed capabilities with a programming model that is similar to 
running threads. Weave is **NOT** a replacement for Apache YARN.  It is instead a value-added framework that operates on top of Apache YARN.

Why do I need Weave?
=====================
Weave dramatically simplifies and reduces your development efforts, enabling you to quickly and easily manage 
your distributed applications through its friendly abstraction layer built on YARN. YARN can normally be quite difficult to use and requires a large ramp up effort, especially since it is built only for MapReduce and is normally meant only for managing batch jobs. YARN, however, can be used as a generalized custom resource management tool that can run any type of job, and could possibly be used for batch jobs, real time jobs, and long running jobs. Unfortunately, YARN’s capabilities are too low level to allow you to quickly develop an application, requiring a great deal of boilerplate code even for simple applications, and its logging output does not become available until the application is finished. This becomes an especially serious issue when managing long running jobs: since those jobs never finish you cannot view the logs, which makes it very difficult to develop and debug such applications. Finally, YARN does not provide standard support for application lifecycle management, communication between containers and the Application Master, and handling application level errors. Continuuity Weave empowers you, the developer, to quickly and easily manage your distributed applications with a much simpler programming model. Using Continuuity Weave, you can easily and quickly build, test, run, and debug your YARN applications, dramatically reducing your development effort and simplifying the management of resources and jobs in your distributed applications.

Continuuity Weave provides you with the following benefits:

  * A simplified API for specifying, running and managing applications
  * A simplified way to specify and manage the stages of the application lifecycle
  * A generic Application Master to better support simple applications
  * Simplified archive management
  * Log and metrics aggregation for your applications, with improved control over application logs, metrics and errors
  * Discovery service
  
To get started with Weave, visit http://continuuity.github.io/weave/.

