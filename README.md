# Spark-Core Lib
The goal of this project is to fulfil the user needs over the standard libs of [Apache Spark](http://spark.apache.org/).
This project is maintained from the [DBTrento group](http://db.disi.unitn.eu) of the University of Trento.

To use this library, edit your pom.xml to match the following

```<project ...>
<repositories>
    <repository>
      <id>java.net</id>
      <url>https://raw.github.com/forons/spark-core/maven-branch/</url>
    </repository>
 </repositories>
...
</project>
```
Then include the artifact as follows

<project>
...
 <dependencies>
   <dependency>
     <groupId>eu.unitn.disi.db</groupId>
     <artifactId>spark-core</artifactId>
     <version>1.0-SNAPSHOT</version>
   </dependency>
 </dependencies>
 ...
</project>



## TO-DO
* Check io package with tests
* Check query executor with tests

## Developers 
* [Daniele Foroni](http://disi.unitn.it/~foroni)
* Paolo Sottovia
* Giulia Preti

For any request write an email at `daniele (dot) foroni (at) unitn (dot) it`.
