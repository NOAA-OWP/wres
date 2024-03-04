package wres.reading.waterml.variable;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A variable code.
 */
public class VariableCode implements Serializable
{
    private static final long serialVersionUID = 4782260215264929265L;

    /** Value. */
    private String value;
    /** Network. */
    private String network;
    /** Vocabulary. */
    private String vocabulary;
    /** Variable ID. */
    private Integer variableID;
    /** Default value. */
    @JsonProperty( "default" )
    private Boolean defaultValue;

    /**
     * @return the value
     */
    public String getValue()
    {
        return value;
    }

    /**
     * Sets the value.
     * @param value the value
     */
    public void setValue( String value )
    {
        this.value = value;
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
     * Sets the vocabulary.
     * @param vocabulary the vocabulary
     */
    public void setVocabulary( String vocabulary )
    {
        this.vocabulary = vocabulary;
    }

    /**
     * @return the variable identifier
     */
    public Integer getVariableID()
    {
        return variableID;
    }

    /**
     * Sets the variable identifier.
     * @param variableID the variable identifier
     */
    public void setVariableID( Integer variableID )
    {
        this.variableID = variableID;
    }

    /**
     * @return the default value
     */
    public Boolean getDefaultValue()
    {
        return defaultValue;
    }

    /**
     * Sets the default value.
     * @param defaultValue the default value
     */
    public void setDefaultValue( Boolean defaultValue )
    {
        this.defaultValue = defaultValue;
    }
}
