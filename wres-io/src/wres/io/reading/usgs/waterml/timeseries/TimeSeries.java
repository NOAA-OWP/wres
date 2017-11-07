package wres.io.reading.usgs.waterml.timeseries;

import wres.io.reading.usgs.waterml.variable.Variable;

public class TimeSeries
{
    SourceInfo sourceInfo;
    Variable variable;

    public SourceInfo getSourceInfo()
    {
        return sourceInfo;
    }

    public void setSourceInfo( SourceInfo sourceInfo )
    {
        this.sourceInfo = sourceInfo;
    }

    public Variable getVariable()
    {
        return variable;
    }

    public void setVariable( Variable variable )
    {
        this.variable = variable;
    }

    public TimeSeriesValues[] getValues()
    {
        return values;
    }

    public void setValues( TimeSeriesValues[] values )
    {
        this.values = values;
    }

    public String getName()
    {
        return name;
    }

    public void setName( String name )
    {
        this.name = name;
    }

    TimeSeriesValues[] values;
    String name;
}
