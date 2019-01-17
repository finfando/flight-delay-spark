# Repository information

Delivery is folder that we are going to deliver. PDF report has to be included there.

# How to run

1. Go inside the app catalog

    cd .\delivery\app

2. Build jar file using maven

    mvn package

3. Run spark-submit job providing input file as first parameter and output folder as second parameter.

    spark-submit --master local --class eit_group.App .\target\eit_artifact-1.0-SNAPSHOT.jar C:\temp\input\2008\2008 C:\temp\output\