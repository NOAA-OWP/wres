# Jesse's Java Collections and streams experiments

General idea is use Java 8 streams, have a simple data model.

First experiment was to condition XML data (events) AS they came in.

First is achieved in ReadFewsDataStream.java.

Second experiment was to compare speed of EVS with the approach found above.

Second is achieved in ReadAscData.java.

## To run the XML experiment

    cd src
    javac gov/noaa/wres/pixml/package-info.java
    javac gov/noaa/wres/ReadFewsDataStream.java

    java gov.noaa.wres.ReadFewsDataStream

    cd ..

## To run the ~12m pairs experiment

    cd src
    javac gov/noaa/wres/ReadAscData.java
    java -Xmx4g gov.noaa.wres.ReadAscData

    cd ..

