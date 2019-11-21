#Sample dyno client app

This is by no means how you should setup production, but will help you to start.

##Pre-Requisite
You should have dynomite cluster running or have Dynomite docker running. 
Also you have to gather node information and token(s) 


to test with locally run docker, run:
```java
./gradlew run --args='--name test-cluster'
```

this should print:
```
[main] INFO com.clarivate.server.ServerRunner - setting a value....
[main] INFO com.clarivate.server.ServerRunner - reading a value: This is my test value
```