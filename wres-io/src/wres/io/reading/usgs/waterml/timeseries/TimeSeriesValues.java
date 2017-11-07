package wres.io.reading.usgs.waterml.timeseries;

public class TimeSeriesValues
{
    public TimeSeriesValue[] getValue()
    {
        return value;
    }

    public void setValue( TimeSeriesValue[] value )
    {
        this.value = value;
    }

    public Qualifier[] getQualifier()
    {
        return qualifier;
    }

    public void setQualifier( Qualifier[] qualifier )
    {
        this.qualifier = qualifier;
    }

    public String[] getQualityControlLevel()
    {
        return qualityControlLevel;
    }

    public void setQualityControlLevel( String[] qualityControlLevel )
    {
        this.qualityControlLevel = qualityControlLevel;
    }

    public Method[] getMethod()
    {
        return method;
    }

    public void setMethod( Method[] method )
    {
        this.method = method;
    }

    public String[] getSource()
    {
        return source;
    }

    public void setSource( String[] source )
    {
        this.source = source;
    }

    public String[] getOffset()
    {
        return offset;
    }

    public void setOffset( String[] offset )
    {
        this.offset = offset;
    }

    public String[] getSample()
    {
        return sample;
    }

    public void setSample( String[] sample )
    {
        this.sample = sample;
    }

    public String[] getCensorCode()
    {
        return censorCode;
    }

    public void setCensorCode( String[] censorCode )
    {
        this.censorCode = censorCode;
    }

    TimeSeriesValue[] value;
    Qualifier[] qualifier;
    String[] qualityControlLevel;
    Method[] method;
    String[] source;
    String[] offset;
    String[] sample;
    String[] censorCode;
}
