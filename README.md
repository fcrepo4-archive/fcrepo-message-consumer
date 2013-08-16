This is a fcrepo 4.x indexer that listens to the Fedora JMS topic, retrieves the updated object from the repository once, and then passes the content on to any number of registered handlers.  It is built relying heavily on Spring machinery, including:

* spring-lang
* spring-jms
* activemq-spring (?)


## Running the indexer

Beats me. I assume we should have a stand-alone mode that implements a main() method, but probably should also do something so it runs inside e.g. an OSGi container or Tomcat itself.

## Configuring the indexer
There is an example Spring configuration [in the tests](https://github.com/futures/fcrepo-jms-indexer-solr/blob/master/src/test/resources/spring-test/solr-indexer.xml), but it goes something like this:

```xml
  <!-- ActiveMQ queue to listen for events -->
  <bean id="destination" class="org.apache.activemq.command.ActiveMQTopic">
    <constructor-arg value="fedora" />
  </bean>

  <!-- fcrepo 4 repository to retrieve metadata from -->
  <bean class="org.fcrepo.indexer.RepositoryProfile">
    <property name="repositoryURL" value="http://localhost:${test.port:8080}/rest" />
  </bean>

  <!-- sparql-update indexer -->
<!--
  <bean id="sparqlUpdate" class="org.fcrepo.indexer.sparql.SparqlUpdateIndexer">
    <property name="updateURL" value="http://localhost:${test.port:8080}/openrdf-sesame/test" />
    <property name="updateProperty" value="update"/>
    <property name="subjectPrefix" value="http://foo.edu/object/"/>
  </bean>
-->

  <!-- file serializer -->
  <bean id="fileSerializer" class="org.fcrepo.indexer.FileSerializer">
    <property name="path" value="/tmp/"/>
  </bean>

  <!-- Message Driven POJO (MDP) that manages individual indexers -->
  <bean id="indexerGroup" class="org.fcrepo.indexer.solr.IndexerGroup" />
    <property name="repositoryURL" value="http://localhost:${test.port:8080}/rest" />
    <property name="indexers">
      <set>
        <ref bean="fileSerializer"/>
<!--
        <ref bean="sparqlUpdate"/>
-->
      </set>
    </property>
  </bean>

  <!-- and this is the message listener container -->
  <bean id="jmsContainer" class="org.springframework.jms.listener.DefaultMessageListenerContainer">
    <property name="connectionFactory" ref="connectionFactory"/>
    <property name="destination" ref="destination"/>
    <property name="messageListener" ref="indexerGroup" />
    <property name="sessionTransacted" value="true"/>
  </bean>
```

The magic is in the ```jmsContainer``` bean. It will listen to the ```destination``` for messages, and pass them onto our ```messageListener```. In this case, the ```messageListener``` is a Ruby script that is loaded in the ```lang:jruby``` construct.

The ```messageListener``` class implements the ```ScriptingSolrIndexer``` interface (which implements the ```javax.jms.MessageListener``` interface). It should respond to an ```onMessage``` call by taking the message and indexing the object appropriately.

