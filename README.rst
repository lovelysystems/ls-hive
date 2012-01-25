===========================
Lovely Systems Hive Goodies
===========================

This project is a collection of UDFs used and/or created by Lovely
Systems.

Maven
=====

To use this project in with maven follow the steps described at
https://github.com/lovelysystems/maven

Deployment
==========

The distributionManagement section in the pom contains the actual
repository urls on github. It will lead to an error if you try to
deploy to those urls, because these are no Maven API endpoints, where
maven could upload the artifacts.

So to deploy to the Lovely Systems Maven repository first clone
https://github.com/lovelysystems/maven to your local machine and set
the deployment target location on the commandline like this::

 mvn -DaltDeploymentRepository=snapshot-repo::default::file:../maven/snapshots clean deploy

After deployment simply commit the changes in the maven repository
project and push.

This approach was take from the very useful blog entry at
http://cemerick.com/2010/08/24/hosting-maven-repos-on-github/

