package wres.io.reading.wrds;

import java.util.StringJoiner;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * The parameter codes.
 */

@JsonIgnoreProperties( ignoreUnknown = true )
public class ParameterCodes
{
    private String physicalElement;
    private String duration;
    private String typeSource;
    private String extremum;
    private String probability;

    /**
     * @return the duration
     */
    public String getDuration()
    {
        return duration;
    }

    /**
     * Sets the duration.
     * @param duration the duration
     */
    public void setDuration( String duration )
    {
        this.duration = duration;
    }

    /**
     * @return the physical element
     */
    public String getPhysicalElement()
    {
        return physicalElement;
    }

    /**
     * Sets the physical element.
     * @param physicalElement the physical element
     */
    public void setPhysicalElement( String physicalElement )
    {
        this.physicalElement = physicalElement;
    }

    /**
     * @return the type source
     */
    public String getTypeSource()
    {
        return typeSource;
    }

    /**
     * Sets the type source.
     * @param typeSource the type source
     */
    public void setTypeSource( String typeSource )
    {
        this.typeSource = typeSource;
    }

    /**
     * @return the extremum
     */
    public String getExtremum()
    {
        return extremum;
    }

    /**
     * Sets the extremum.
     * @param extremum the extremum
     */
    public void setExtremum( String extremum )
    {
        this.extremum = extremum;
    }

    /**
     * @return the probability
     */
    public String getProbability()
    {
        return probability;
    }

    /**
     * Sets the probability.
     * @param probability the probability
     */
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
}
