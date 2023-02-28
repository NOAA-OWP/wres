package wres.io.reading.waterml.timeseries;

import java.io.Serializable;

/**
 * A qualifier.
 */
public class Qualifier implements Serializable
{
    private static final long serialVersionUID = -3380625359503743133L;

    /** Qualifier code. */
    private String qualifierCode;
    /** Qualifier description. */
    private String qualifierDescription;
    /** Qualifier ID.*/
    private Integer qualifierID;
    /** Network. */
    private String network;
    /** Vocabulary. */
    private String vocabulary;

    /**
     * @return the qualifier code
     */
    public String getQualifierCode()
    {
        return qualifierCode;
    }

    /**
     * Sets the qualifier code.
     * @param qualifierCode the qualifier code
     */
    public void setQualifierCode( String qualifierCode )
    {
        this.qualifierCode = qualifierCode;
    }

    /**
     * @return the qualifier description
     */
    public String getQualifierDescription()
    {
        return qualifierDescription;
    }

    /**
     * Sets the qualifier description.
     * @param qualifierDescription the qualifier description
     */
    public void setQualifierDescription( String qualifierDescription )
    {
        this.qualifierDescription = qualifierDescription;
    }

    /**
     * @return the qualifier ID
     */
    public Integer getQualifierID()
    {
        return qualifierID;
    }

    /**
     * Sets the qualifier ID.
     * @param qualifierID the qualifier ID
     */
    public void setQualifierID( Integer qualifierID )
    {
        this.qualifierID = qualifierID;
    }

    /**
     * @return the network
     */
    public String getNetwork()
    {
        return network;
    }

    /**
     * Sets the network.
     * @param network the network
     */
    public void setNetwork( String network )
    {
        this.network = network;
    }

    /**
     * @return the vocabulary
     */
    public String getVocabulary()
    {
        return vocabulary;
    }

    /**
     * Sets the vocabulary
     * @param vocabulary the vocabulary
     */
    public void setVocabulary( String vocabulary )
    {
        this.vocabulary = vocabulary;
    }
}
