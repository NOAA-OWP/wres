package wres.io.reading.wrds;

public class ForecastUnits
{
    // Use this rather than 'getFlow' because that will probably be renamed.
    public String getUnitName()
    {
        return this.flow;
    }

    public String getFlow()
    {
        return flow;
    }

    public void setFlow( String flow )
    {
        this.flow = flow;
    }

    String flow;
}
