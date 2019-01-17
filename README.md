Build jar file

    mvn package

Running program on Windows

    C:\Installations\spark-2.4.0-bin-hadoop2.7\bin\spark-submit.cmd --master local --class eit_group.App C:\Users\Filip\gitrepos\spark_project\app\target\eit_artifact-1.0-SNAPSHOT.jar /temp/input/spark_test.txt /temp/output