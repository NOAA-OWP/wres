package wres.io.reading.waterml.timeseries;

import wres.io.reading.waterml.variable.Variable;

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

    public boolean isPopulated()
    {
        if (this.getValues().length > 0)
        {
            for (TimeSeriesValues values : this.getValues())
            {
                if (values.getValue().length > 0)
                {
                    return true;
                }
            }
        }

        return false;
    }

    TimeSeriesValues[] values;
    String name;
}
