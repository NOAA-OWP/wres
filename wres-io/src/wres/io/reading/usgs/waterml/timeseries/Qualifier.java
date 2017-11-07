package wres.io.reading.usgs.waterml.timeseries;

public class Qualifier
{
    String qualifierCode;

    public String getQualifierCode()
    {
        return qualifierCode;
    }

    public void setQualifierCode( String qualifierCode )
    {
        this.qualifierCode = qualifierCode;
    }

    public String getQualifierDescription()
    {
        return qualifierDescription;
    }

    public void setQualifierDescription( String qualifierDescription )
    {
        this.qualifierDescription = qualifierDescription;
    }

    public Integer getQualifierID()
    {
        return qualifierID;
    }

    public void setQualifierID( Integer qualifierID )
    {
        this.qualifierID = qualifierID;
    }

    public String getNetwork()
    {
        return network;
    }

    public void setNetwork( String network )
    {
        this.network = network;
    }

    public String getVocabulary()
    {
        return vocabulary;
    }

    public void setVocabulary( String vocabulary )
    {
        this.vocabulary = vocabulary;
    }

    String qualifierDescription;
    Integer qualifierID;
    String network;
    String vocabulary;
}
