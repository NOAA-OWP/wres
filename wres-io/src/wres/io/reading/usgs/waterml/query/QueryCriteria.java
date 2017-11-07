package wres.io.reading.usgs.waterml.query;

public class QueryCriteria
{
    public String getLocationParam()
    {
        return locationParam;
    }

    public void setLocationParam( String locationParam )
    {
        this.locationParam = locationParam;
    }

    public String getVariableParam()
    {
        return variableParam;
    }

    public void setVariableParam( String variableParam )
    {
        this.variableParam = variableParam;
    }

    public QueryTimeParameter getTimeParam()
    {
        return timeParam;
    }

    public void setTimeParam( QueryTimeParameter timeParam )
    {
        this.timeParam = timeParam;
    }

    public String[] getParameter()
    {
        return parameter;
    }

    public void setParameter( String[] parameter )
    {
        this.parameter = parameter;
    }

    String locationParam;
    String variableParam;
    QueryTimeParameter timeParam;
    String[] parameter;
}
