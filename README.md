This is a fcrepo 4.x indexer that listens to the Fedora JMS topic, retrieves the updated object from the repository once, and then passes the content on to any number of registered handlers.  It is built relying heavily on Spring machinery, including:

* spring-lang
* spring-jms
* activemq-spring (?)


## Running the indexer

We should have both a stand-alone mode where the indexer is run separately from the repository, and a bundled mode where the indexer is deployed with the repository.

## Configuring the indexer

There is an example Spring configuration [in the tests](https://github.com/futures/fcrepo-jms-indexer-solr/blob/master/src/test/resources/spring-test/solr-indexer.xml), but it goes something like this:

```xml
  <!-- sparql-update indexer -->
  <bean id="sparqlUpdate" class="org.fcrepo.indexer.SparqlIndexer">
    <!-- base URL for triplestore subjects, PID will be appended -->
    <property name="prefix" value="http://localhost:${test.port:8080}/rest/objects/"/>

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

  <!-- file serializer -->
  <bean id="fileSerializer" class="org.fcrepo.indexer.FileSerializer">
    <property name="path" value="./target/test-classes/fileSerializer/"/>
  </bean>

  <!-- Message Driven POJO (MDP) that manages individual indexers -->
  <bean id="indexerGroup" class="org.fcrepo.indexer.IndexerGroup">
    <property name="repositoryURL" value="http://localhost:${test.port:8080}/rest/objects/" />
    <property name="indexers">
      <set>
        <ref bean="fileSerializer"/>
        <ref bean="sparqlUpdate"/>
      </set>
    </property>
  </bean>

  <!-- ActiveMQ queue to listen for events -->
  <bean id="destination" class="org.apache.activemq.command.ActiveMQTopic">
    <constructor-arg value="fedora" />
  </bean>

  <!-- and this is the message listener container -->
  <bean id="jmsContainer" class="org.springframework.jms.listener.DefaultMessageListenerContainer">
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

## Caveat: Blank Nodes

Currently, blank nodes are not supported and will cause deletes and updates to fail, because the SPARQL Update protocol does not have support for deleting blank nodes.  By default, no blank nodes are generated by Fedora, so this should not typically be a problem.  In cases where blank nodes are attached to objects (e.g., to implement descriptive metadata schemas such as MADS), an alternative indexer will need to be created.  This indexer would need to use a native API tied to a particular triplestore in order to access and delete blank nodes.
