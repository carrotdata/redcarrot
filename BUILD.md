## Build prerequisits

Java 11 +, Gnu cc compiler (any recent version will work), maven 3.x.

## Carrot source build instructions:

- set JAVA_HOME environment variables you can set JAVA_HOME anywhere start script or IDE but recommended in Mac/Linux
  start script in this case build will use same settings. for example in you .bashrc file at the end add following:

export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 export PATH=$JAVA_HOME/bin:$PATH

- run the following command to build Carrot:

```
$ mvn clean install -DskipTests
```

To create Eclipse environment files:

```
$ mvn --settings settings.xml eclipse:eclipse -DskipTests
```

