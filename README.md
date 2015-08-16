fcrepo-message-consumer
=======================

[![Build Status](https://travis-ci.org/fcrepo4-exts/fcrepo-message-consumer.png?branch=master)](https://travis-ci.org/fcrepo4-exts/fcrepo-message-consumer)


This is a fcrepo 4.x indexer that listens to the Fedora JMS topic, retrieves a message including pid and eventType, looks up object properties, gets and passes the transformed or untransformed properties on to any number of registered handlers.  It is built relying heavily on Spring machinery, including:

* spring-lang
* spring-jms
* activemq-spring (?)


## Running the indexer

In the simplest case, the indexer can be configured in the same container as the repository.   See [kitchen-sink/fuseki](https://github.com/fcrepo4-labs/fcrepo-kitchen-sink/tree/fuseki) for an example of this configuration.

For production deployment, it is more typical to run the indexer on a separate machine.  So we also have a stand-alone mode where the indexer is run as its own webapp:

```xml
$ git clone https://github.com/futures/fcrepo-jms-indexer-pluggable.git
$ cd fcrepo-jms-indexer-pluggable/fcrepo-jms-indexer-webapp
$ mvn -D jetty.port=9999 install jetty:run
```

## Configuring the indexer

 [Test Spring Configuration](https://github.com/fcrepo4-exts/fcrepo-message-consumer/tree/master/fcrepo-message-consumer-core/src/test/resources/spring-test)
 
 [Production Spring Configuration](https://github.com/fcrepo4-exts/fcrepo-message-consumer/tree/master/fcrepo-message-consumer-webapp/src/main/resources/spring) 

indexer-core.xml
```xml
  <!-- sparql-update indexer -->
  <bean id="sparqlUpdate" class="org.fcrepo.indexer.SparqlIndexer">
    <!-- base URL for triplestore subjects, PID will be appended -->
    <property name="prefix" value="http://localhost:${fcrepo.dynamic.test.port:8080}/rest/objects/"/>

    <!-- fuseki (used by tests) -->
    <property name="queryBase" value="http://localhost:3030/test/query"/>
    <property name="updateBase" value="http://localhost:3030/test/update"/>
    <property name="formUpdates">
      <value type="java.lang.Boolean">false</value>
    </property>

    <!-- sesame -->
    <!--
    <property name="queryBase" value="http://localhost:8080/openrdf-sesame/repositories/test"/>
    <property name="updateBase" value="http://localhost:8080/openrdf-sesame/repositories/test/statements"/>
    <property name="formUpdates">
      <value type="java.lang.Boolean">true</value>
    </property>
    -->
  </bean>
  
  <!--Embedded Server used in spring-test -->
  <!--
  
  <bean id="multiCore" class="org.apache.solr.core.CoreContainer"
    factory-method="createAndLoad" c:solrHome="target/test-classes/solr"
    c:configFile-ref="solrConfig"/>
    
  <bean class="java.io.File" id="solrConfig">
    <constructor-arg type="String">
      <value>target/test-classes/solr/solr.xml</value>
    </constructor-arg>
  </bean>

  <bean id="solrServer"
    class="org.apache.solr.client.solrj.embedded.EmbeddedSolrServer"
    c:coreContainer-ref="multiCore" c:coreName="testCore"/>
    -->
  <!-- end Embedded Server-->
  
  <!--Standardalone solr Server -->
  <bean id="solrServer" class="org.apache.solr.client.solrj.impl.HttpSolrServer">
    <constructor-arg index="0" value="http://${fcrepo.host:localhost}:${solrIndexer.port:8983}/solr/" />
  </bean>
  
  <!-- Solr Indexer START-->
    <bean id="solrIndexer" class="org.fcrepo.indexer.solr.SolrIndexer">
    <constructor-arg ref="solrServer" />
    </bean>

  <!-- file serializer -->
  <bean id="fileSerializer" class="org.fcrepo.indexer.FileSerializer">
    <property name="path" value="./target/test-classes/fileSerializer/"/>
  </bean>

  <!-- Message Driven POJO (MDP) that manages individual indexers -->
  <bean id="indexerGroup" class="org.fcrepo.indexer.IndexerGroup">
    <property name="repositoryURL" value="http://localhost:${fcrepo.dynamic.test.port:8080}/rest/objects/" />
    <property name="indexers">
      <set>
        <ref bean="sparqlUpdate"/>
        <ref bean="solrIndexer"/>
        <ref bean="fileSerializer"/>
      </set>
    </property>
  </bean>
  <!--end indexer-core.xml-->
```

Here 3 indexers are implemented, sparqlUpdate writing to an as configured fuseki triplestore, solrIndexer writing to an as configured standalone solr instance, and fileSerializer writing to an arbitrary path.

indexer-events.xml
```xml
  <bean id="connectionFactory"
    class="org.apache.activemq.ActiveMQConnectionFactory">
    <property name="brokerURL" value="vm://localhost"/>
  </bean>

  <bean id="pooledConnectionFactory"
    class="org.apache.activemq.pool.PooledConnectionFactory"
    depends-on="connectionFactory">
    <property name="connectionFactory" ref="connectionFactory"/>
    <property name="maxConnections" value="1"/>
    <property name="idleTimeout" value="0"/>
  </bean>
  
  <!-- ActiveMQ queue to listen for events -->
  <bean id="destination" class="org.apache.activemq.command.ActiveMQTopic">
    <constructor-arg value="fedora" />
  </bean>

  <!-- and this is the message listener container -->
  <bean id="jmsContainer" class="org.springframework.jms.listener.DefaultMessageListenerContainer"
    depends-on="destination, pooledConnectionFactory">
    <property name="connectionFactory" ref="connectionFactory"/>
    <property name="destination" ref="destination"/>
    <property name="messageListener" ref="indexerGroup" />
    <property name="sessionTransacted" value="true"/>
  </bean>
```

The magic is in the ```jmsContainer``` bean. It listens to the ```destination``` for messages, and pass them onto our ```messageListener```.  The ```messageListener``` retrieves the Fedora object from the repo (for adds/updates) and passes the pid and content to each indexer class defined in the ```indexers``` set.

## Dependencies

Currently, the tests work with either Jena Fuseki or Sesame triplestores/SPARQL servers.  To switch between them, edit ```src/test/resources/spring-test/indexer-core.xml```.

### Fuseki
Fuseki is the easiest to setup -- just download it from http://www.apache.org/dist/jena/binaries/, unpack and start ```fuseki-server```:

``` sh
curl -O http://www.apache.org/dist/jena/binaries/jena-fuseki-0.2.7-distribution.tar.gz
tar xvfz jena-fuseki-0.2.7-distribution.tar.gz
cd jena-fuseki-0.2.7
./fuseki-server --update --mem /test
```

### Sesame

Sesame requires a little more setup to run with the tests, since by default it uses the same port as Fedora.  To setup Sesame with Tomcat running on an alternate port:

* Download Sesame from http://sourceforge.net/projects/sesame/files/Sesame%202/ 
* Download Tomcat from http://tomcat.apache.org/download-70.cgi* Unpack Sesame and Tomcat, and move the Sesame WAR file into the Tomcat webapps directory
* Change the Tomcat port to something other than 8080 to avoid conflict with Fedora, and then start Tomcat.
* Use the Sesame console to create a repository

    ``` sh
    curl -L -O http://downloads.sourceforge.net/project/sesame/Sesame%202/2.7.5/openrdf-sesame-2.7.5-sdk.tar.gz
    curl -O http://www.apache.org/dist/tomcat/tomcat-7/v7.0.42/bin/apache-tomcat-7.0.42.tar.gz
    tar xvfz apache-tomcat-7.0.42.tar.gz
    tar xvfz openrdf-sesame-2.7.5-sdk.tar.gz
    cp openrdf-sesame-2.7.5/war/openrdf-sesame.war apache-tomcat-7.0.42/webapps/
    cat apache-tomcat-7.0.42/conf/server.xml | sed -e's/8080/${tomcat.port}/' > tmp.xml
    mv tmp.xml apache-tomcat-7.0.42/conf/server.xml
    export CATALINA_HOME=`pwd`/apache-tomcat-7.0.42
    export JAVA_OPTS="$JAVA_OPTS -Dtomcat.port=8081"
    apache-tomcat-7.0.42/bin/startup.sh
    openrdf-sesame-2.7.5/bin/console.sh
    > connect http://localhost:8081/openrdf-sesame.
    > create native.
    Repository ID [native]: test
    Repository title [Native store]: test
    Triple indexes [spoc,posc]: spoc,posc
    > quit.
    ```

### Solr

Solr can be installed embedded into a jetty server (recommended for test) or in a tomcat container (recommended for production).  Download install and configuration are here: https://cwiki.apache.org/confluence/display/solr/Getting+Started

### Maven Build

Use the following MAVEN_OPTS on build

   ``` sh
   MAVEN_OPTS="-Xmx750M -XX:MaxPermSize=300M" mvn clean install
   ```

### Caveat: Blank Nodes

Fedora doesn't currently support blank nodes.

## Authenticated repo

If REST calls to your Fedora repository require BASIC authentication,
you'll need to set two system variables in your servlet container,
`fcrepo.username` and `fcrepo.password`. In Jetty/Maven 3, you can set
some values in your [settings.xml](https://maven.apache.org/settings.html)
file that will later be set to these two system variables:

``` xml
<profiles>
  <profile>
    <id>fcrepo</id>
    <activation>
      <activeByDefault>true</activeByDefault>
    </activation>
    <properties>
      <fcrepo.username>example</fcrepo.username>
      <fcrepo.password>xxxxxxxx</fcrepo.password>
    </properties>
  </profile>
</profiles>
```

In Tomcat 7 you can set the following command line options in
your `conf/setenv.sh` file:

``` sh
JAVA_OPTS="$JAVA_OPTS -Dfcrepo.username=example -Dfcrepo.password=xxxxxxxx "
```
