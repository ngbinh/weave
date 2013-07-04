What is Weave ?
===============

Weave is a simple set of libraries that allows you to easily manage distributed applications through an abstraction layer 
built on Apache YARN. Weave allows you to use YARN’s distributed capabilities with a programming model that is similar to 
running threads. Weave is **NOT** a replacement for Apache YARN. Weave is a value-added framework that operates on top of Apache YARN.

Why Do I Need Weave?
=====================
Weave dramatically simplifies and reduces your development efforts, enabling you to quickly and easily manage 
your distributed applications through its friendly abstraction layer built on YARN. YARN can normally be quite difficult to use and requires a big ramp up effort, especially since it's only built for MapReduce and is normally meant for managing batch jobs. YARN, however, can be used as a generalized custom resource management tool that can run any type of job including batch jobs, real-time jobs, and long-running jobs. 

Unfortunately, YARN’s capabilities are too low level to allow you to quickly develop applications. YARN requires a great deal of boilerplate code even for simple applications, and its logging output does not become available until the application is finished. This becomes especially problamatic when managing long-running jobs. Because those jobs never finish you cannot view their logs, which makes it very difficult to develop and debug them. Finally, YARN does not provide standard support for application lifecycle management, communications between containers and the Application Master, and handling application level errors. 

Weave allows you to manage your distributed applications with a much simpler programming model. You can quickly and easily build, test, run, and debug YARN applications, dramatically reducing your development efforts and simplifying the management of resources and jobs in your distributed applications.

Weave provides you with the following benefits:

  * A simplified API for specifying, running and managing applications
  * A simplified way to specify and manage the stages of the application lifecycle
  * A generic Application Master to better support simple applications
  * Simplified archive management
  * Log and metrics aggregation for your applications, with improved control over application logs, metrics and errors
  * A discovery service
  
To get started with Weave, visit http://continuuity.github.io/weave/.

Community
=========
How to Contribute
------------------

Interested in improving Weave? We have a simple pull-based development model with a consensus-building phase, similar to Apache's voting process. If you’d like to help make Weave better by adding new features, enhancing existing features, or fixing bugs, here's how to do it:

  * Fork weave into your own GitHub repository
  * Create a topic branch with an appropriate name
  * Work on the code to your heart's content
  * Once you’re satisfied, create a pull request from your repo (it’s helpful if you fill in all of the description fields)
  * After we review and accept your request we’ll commit your code to the continuuity/weave repo
  * Thanks!

Groups
------
User Group: https://groups.google.com/d/forum/weave-user

License
=======
Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
