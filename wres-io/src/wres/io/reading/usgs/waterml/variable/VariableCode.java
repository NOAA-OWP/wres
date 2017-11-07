package wres.io.reading.usgs.waterml.variable;

import com.fasterxml.jackson.annotation.JsonProperty;

public class VariableCode
{
    String value;
    String network;

    public String getValue()
    {
        return value;
    }

    public void setValue( String value )
    {
        this.value = value;
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

    public Integer getVariableID()
    {
        return variableID;
    }

    public void setVariableID( Integer variableID )
    {
        this.variableID = variableID;
    }

    public Boolean getDefaultValue()
    {
        return defaultValue;
    }

    public void setDefaultValue( Boolean defaultValue )
    {
        this.defaultValue = defaultValue;
    }

    String vocabulary;
    Integer variableID;

    @JsonProperty("default")
    Boolean defaultValue;
}
