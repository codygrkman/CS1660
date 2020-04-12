# Inverted Indexing Project

#### YouTube Demo Link

https://youtu.be/hwgRGvq0Hp8

#### What I Implemented

The following items were implemented from the Project Grading Criteria:

| Criteria | Implemented? |
| ------ | ------ |
| First Java Application Implementation and Execution on Docker | Yes |
| Docker to Local (or GCP) Cluster Communication | Yes |
| Inverted Indexing MapReduce Implementation and Execution on the Cluster (GCP) | Yes |
| Term and Top-N Search | No |
| Implementation of Custom Functionality (e.g. Hadoop counters, logging, etc... | No |
| Implementation of Secondary Sorting Algorithm with Inverted Indexing | No |

Implemented in addition to grading criteria:

    -- Ability to run Inverted Index program on uploaded files. i.e. the job runs on whatever file is uploaded, not just the 3 arbitrary ones provided as examples.

#### About

This repository hosts the source code from my final project for my Cloud Computing Course, CS1660.
The application is written in Java and utilizes an Apache Hadoop instance running on Google Cloud Platform.

The application is written as a Java Maven app with a Swing GUI. To run it on non-X11 compatible systems, you must use Xming as described in this guide: https://docs.microsoft.com/en-us/archive/blogs/jamiedalton/windows-10-docker-gui

In order to run the GCP-side code, you must have InvertedIndexJob.java uploaded as a JAR file to your GCP Dataproc Apache Hadoop instance.

For authentication, you must obtain a JSON service key from Google Cloud Platform and set an environment variable named $GOOGLE_APP_CREDENTIALS to the path of this service key.


#### Docker Commands
Build:
```sh
$ docker build -t my-java-app .
```
Run:
```sh
$ docker run -it --privileged -e DISPLAY=$DISPLAY -e GOOGLE_APPLICATION_CREDENTIALS=$GOOGLE_APPLICATION_CREDENTIALS my-java-app
```
