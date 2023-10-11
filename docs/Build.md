Prerequisites:
  you must install git, maven, and a jdk

To build the project, start with your working directory in the root of the git repository.

```bash
~/movement$ mvn -DskipTests clean package
...
[INFO] ------------------------------------------------------------------------
[INFO] Reactor Summary for Movement 1.0.0-SNAPSHOT:
[INFO] 
[INFO] Movement ........................................... SUCCESS [  0.113 s]
[INFO] core ............................................... SUCCESS [  3.645 s]
[INFO] plugin ............................................. SUCCESS [  0.139 s]
[INFO] extensions ......................................... SUCCESS [  0.001 s]
[INFO] generator .......................................... SUCCESS [  0.405 s]
[INFO] tinkerpop .......................................... SUCCESS [  3.279 s]
[INFO] files .............................................. SUCCESS [  0.307 s]
[INFO] cli ................................................ SUCCESS [  0.588 s]
[INFO] performance ........................................ SUCCESS [  0.084 s]
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time:  8.667 s
[INFO] Finished at: 2023-10-10T15:54:13-06:00
[INFO] ------------------------------------------------------------------------
```
