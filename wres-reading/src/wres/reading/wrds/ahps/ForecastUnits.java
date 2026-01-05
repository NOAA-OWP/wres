package wres.reading.wrds.ahps;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Getter;
import lombok.Setter;

/**
 * The forecast units.
 */

@Getter
@Setter
@JsonIgnoreProperties( ignoreUnknown = true )
public class ForecastUnits
{
    private String flow;
    private String streamflow;
    private String stage;

    /**
     * @return the unit name
     */
    public String getUnitName()
    {
        if ( this.hasValue( this.flow ) )
        {
            return this.flow;
        }
        else if ( this.hasValue( this.streamflow ) )
        {
            return this.streamflow;
        }

        return this.stage;
    }

    /**
     * @param word the word to check
     * @return whether the word has some non whitespace characters
     */
    private boolean hasValue( String word )
    {
        return Objects.nonNull( word ) && !word.isBlank();
    }
}
