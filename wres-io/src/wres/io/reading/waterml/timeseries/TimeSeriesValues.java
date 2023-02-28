package wres.io.reading.waterml.timeseries;

import java.io.Serial;
import java.io.Serializable;
import java.time.Duration;

/**
 * A collection of time-=series values.
 */
public class TimeSeriesValues implements Serializable
{
    @Serial
    private static final long serialVersionUID = -23502269417368543L;

    /** Time-series values. */
    private TimeSeriesValue[] value;
    /** Qualifiers. */
    private Qualifier[] qualifier;
    /** Quality control levels. */
    private String[] qualityControlLevel;
    /** Method. */
    private Method[] method;
    /** Source. */
    private String[] source;
    /** Offsets. */
    private String[] offset;
    /** Samples. */
    private String[] sample;
    /** Censor codes. */
    private String[] censorCode;

    /**
     * @return the time-series values
     */
    public TimeSeriesValue[] getValue()
    {
        return value;
    }

    /**
     * Sets the time-series values
     * @param value the values
     */
    public void setValue( TimeSeriesValue[] value )
    {
        this.value = value;
    }

    /**
     * @return the qualifiers
     */
    public Qualifier[] getQualifier()
    {
        return qualifier;
    }

    /**
     * Sets the qualifiers.
     * @param qualifier the qualifiers
     */
    public void setQualifier( Qualifier[] qualifier )
    {
        this.qualifier = qualifier;
    }

    /**
     * @return the quality control level
     */
    public String[] getQualityControlLevel()
    {
        return qualityControlLevel;
    }

    /**
     * Sets the quality control level.
     * @param qualityControlLevel the quality control level
     */
    public void setQualityControlLevel( String[] qualityControlLevel )
    {
        this.qualityControlLevel = qualityControlLevel;
    }

    /**
     * @return the methods
     */
    public Method[] getMethod()
    {
        return method;
    }

    /**
     * Sets the methods.
     * @param method the methods
     */
    public void setMethod( Method[] method )
    {
        this.method = method;
    }

    /**
     * @return the sources
     */
    public String[] getSource()
    {
        return source;
    }

    /**
     * Sets the sources.
     * @param source the sources
     */
    public void setSource( String[] source )
    {
        this.source = source;
    }

    /**
     * @return the offsets
     */
    public String[] getOffset()
    {
        return offset;
    }

    /**
     * Sets the offsets.
     * @param offset the offsets
     */
    public void setOffset( String[] offset )
    {
        this.offset = offset;
    }

    /**
     * @return the sample
     */
    public String[] getSample()
    {
        return sample;
    }

    /**
     * Sets the sample.
     * @param sample the sample
     */
    public void setSample( String[] sample )
    {
        this.sample = sample;
    }

    /**
     * @return the censor code
     */
    public String[] getCensorCode()
    {
        return censorCode;
    }

    /**
     * Sets the censor code.
     * @param censorCode the censor code
     */
    public void setCensorCode( String[] censorCode )
    {
        this.censorCode = censorCode;
    }

    /**
     * @return the time-step
     */
    public Duration getTimeStep()
    {
        Duration step = Duration.ZERO;

        if ( this.value != null && this.value.length > 1 )
        {
            return Duration.between( this.value[0].getDateTime(), this.value[1].getDateTime() );
        }

        return step;
    }
}
