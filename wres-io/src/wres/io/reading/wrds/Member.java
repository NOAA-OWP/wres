package wres.io.reading.wrds;

import java.util.List;

public class Member
{
    String identifier;
    List<List<DataPoint>> dataPointsList;

    public List<List<DataPoint>> getDataPointsList()
    {
        return dataPointsList;
    }

    public String getIdentifier()
    {
        return identifier;
    }

    public void setDataPointsList( List<List<DataPoint>> dataPointsList )
    {
        this.dataPointsList = dataPointsList;
    }

    public void setIdentifier( String identifier )
    {
        this.identifier = identifier;
    }

}
