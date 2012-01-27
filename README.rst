===========================
Lovely Systems Hive Goodies
===========================

This project is a collection of UDFs used and/or created by Lovely
Systems.

UDFs
====

<T> ArrayItemUDF(array<T> arr, int idx):

 returns item of ``arr`` at position ``idx``. If ``idx`` is negative,
 the length of ``arr`` is added to it (so that, e.g., func(x, -1)
 selects the last item of x.)

int ESHashUDF(string k):

 computes the hash Elasticsearch uses for shard allocation for a given
 key

int MemcachedUDF(string servers, string key, string value):

 stores ``value`` under given ``key`` on memcached instances defined
 in ``servers``.

array<string> RegexExtractAllUDF(string haystack, string pattern, int
 group):

 Extracts all matches of the regex group at ``group``
 identified by ``pattern`` from ``haystack``

"int RowNumberUDF(key1, key2, ...):

 Return the row number starting at 1.  Whenever the value of any key
 changes the numbering is reset to 1."

string UnescapeXMLUDF(string src):

 Unescapes the basic xml entitities in ``src``.


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

