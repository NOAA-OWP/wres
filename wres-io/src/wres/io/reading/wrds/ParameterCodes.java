package wres.io.reading.wrds;


import java.util.StringJoiner;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties( ignoreUnknown = true )
public class ParameterCodes
{
    public String getDuration()
    {
        return duration;
    }

    public void setDuration( String duration )
    {
        this.duration = duration;
    }

    public String getPhysicalElement()
    {
        return physicalElement;
    }

    public void setPhysicalElement( String physicalElement )
    {
        this.physicalElement = physicalElement;
    }

    public String getTypeSource()
    {
        return typeSource;
    }

    public void setTypeSource( String typeSource )
    {
        this.typeSource = typeSource;
    }

    public String getExtremum()
    {
        return extremum;
    }

    public void setExtremum( String extremum )
    {
        this.extremum = extremum;
    }

    public String getProbability()
    {
        return probability;
    }

    public void setProbability( String probability )
    {
        this.probability = probability;
    }

    @Override
    public String toString()
    {
        StringJoiner joiner = new StringJoiner( ",", "[", "]" );

        joiner.add( "physicalElement: " + physicalElement )
              .add( "duration: " + duration )
              .add( "typeSource: " + typeSource )
              .add( "extremum: " + extremum )
              .add( "probability: " + probability );

        return joiner.toString();
    }
    
    String physicalElement;
    String duration;
    String typeSource;
    String extremum;
    String probability;
}
