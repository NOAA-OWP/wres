package wres.reading.wrds.ahps;

import java.util.StringJoiner;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Getter;
import lombok.Setter;

/**
 * The parameter codes.
 */

@JsonIgnoreProperties( ignoreUnknown = true )
@Getter
@Setter
public class ParameterCodes
{
    private String physicalElement;
    private String duration;
    private String typeSource;
    private String extremum;
    private String probability;

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
