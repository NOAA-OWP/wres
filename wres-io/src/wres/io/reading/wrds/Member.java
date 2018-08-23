package wres.io.reading.wrds;

public class Member
{
    public DataPoint[] getDataPointsList()
    {
        return dataPointsList;
    }

    public String getIdentifier()
    {
        return identifier;
    }

    public void setDataPointsList( DataPoint[] dataPointsList )
    {
        this.dataPointsList = dataPointsList;
    }

    public void setIdentifier( String identifier )
    {
        this.identifier = identifier;
    }

    String identifier;
    DataPoint[] dataPointsList;
}
