package wres.reading.waterml.timeseries;

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;

import wres.reading.waterml.variable.Variable;

/**
 * A time-series.
 */
public class TimeSeries implements Serializable
{
    @Serial
    private static final long serialVersionUID = -5581319084334610655L;

    /** Source info. */
    private SourceInfo sourceInfo;
    /** Variable. */
    private Variable variable;
    /** Time-series values. */
    private TimeSeriesValues[] values;
    /** Name. */
    private String name;

    /**
     * @return the source information
     */
    public SourceInfo getSourceInfo()
    {
        return sourceInfo;
    }

    /**
     * Sets the source information.
     * @param sourceInfo the source information
     */
    public void setSourceInfo( SourceInfo sourceInfo )
    {
        this.sourceInfo = sourceInfo;
    }

    /**
     * @return the variable
     */
    public Variable getVariable()
    {
        return variable;
    }

    /**
     * Sets the variable.
     * @param variable the variable
     */
    public void setVariable( Variable variable )
    {
        this.variable = variable;
    }

    /**
     * @return the time-series values.
     */
    public TimeSeriesValues[] getValues()
    {
        return values;
    }

    /**
     * Sets the time-series values
     * @param values the time-series values
     */
    public void setValues( TimeSeriesValues[] values )
    {
        this.values = values;
    }

    /**
     * @return the name
     */
    public String getName()
    {
        return name;
    }

    /**
     * Sets the name.
     * @param name the name
     */
    public void setName( String name )
    {
        this.name = name;
    }

    /**
     * @return true if there are time-series values, false otherwise
     */
    public boolean isPopulated()
    {
        if ( Objects.isNull( values ) || this.values.length == 0 )
        {
            return false;
        }

        for ( TimeSeriesValues v : this.getValues() )
        {
            if ( v.getValue().length > 0 )
            {
                return true;
            }
        }

        return false;
    }
}
