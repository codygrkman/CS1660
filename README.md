# Inverted Indexing Project

This repository hosts the source code from my final project for my Cloud Computing Course, CS1660.
The application is written in Java and utilizes an Apache Hadoop instance running on Google Cloud Platform.

The application is written as a Java Maven app with a Swing GUI. To run it on non-X11 compatible systems, you must use Xming as described in this guide: https://docs.microsoft.com/en-us/archive/blogs/jamiedalton/windows-10-docker-gui

In order to run the GCP-side code, you must have InvertedIndexJob.java uploaded as a JAR file to your GCP Dataproc Apache Hadoop instance.

For authentication, you must obtain a JSON service key from Google Cloud Platform and set an environment variable named $GOOGLE_APP_CREDENTIALS to the path of this service key.


#### Docker
Build:
```sh
$ docker build -t my-java-app .
```
Run:
```sh
$ docker run -it --privileged -e DISPLAY=$DISPLAY -e GOOGLE_APPLICATION_CREDENTIALS=$GOOGLE_APPLICATION_CREDENTIALS my-java-app
```
