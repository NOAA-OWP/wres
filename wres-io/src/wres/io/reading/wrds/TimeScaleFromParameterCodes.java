package wres.io.reading.wrds;

import java.net.URI;
import java.time.Duration;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.scale.TimeScaleOuter.TimeScaleFunction;

/**
 * <p>Helper class that builds a {@link TimeScaleOuter} from a set of {@link ParameterCodes}.
 * A {@link TimeScaleOuter} comprises a {@link TimeScaleOuter#getPeriod()} and a 
 * {@link TimeScaleOuter#getFunction()}. The main hint is the 
 * {@link ParameterCodes#getDuration()}, which describes the <code>period</code> 
 * over which the value applies. The <code>function</code> depends on the 
 * {@link ParameterCodes#getPhysicalElement()}. See #60158-11. 
 * 
 * TODO: implement this class more fully, accounting for other types of {@link ParameterCodes#getPhysicalElement()}.
 * 
 * @author james.brown@hydrosolved.com
 */

public class TimeScaleFromParameterCodes
{

    /**
     * Logger.
     */

    private static final Logger LOGGER = LoggerFactory.getLogger( TimeScaleFromParameterCodes.class );

    /**
     * Returns a {@link TimeScaleOuter} from the input parameter codes.
     * 
     * @param parameterCodes the parameter codes
     * @param source a source URI, which is used to help with logging
     * @return a time scale or null if one could not be interpreted
     * @throws NullPointerException if the inputs is null
     * @throws UnsupportedOperationException if the scale is unsupported
     */

    public static TimeScaleOuter getTimeScale( ParameterCodes parameterCodes, URI source )
    {
        Objects.requireNonNull( parameterCodes,
                                "Specify non-null parameter codes alongside the WRDS source '" + source + "'." );

        TimeScaleOuter returnMe = null;

        if ( "I".equalsIgnoreCase( parameterCodes.getDuration() ) )
        {
            returnMe = TimeScaleOuter.of();
        }
        else
        {
            returnMe = TimeScaleFromParameterCodes.getNonInstantaneousTimeScale( parameterCodes, source );
        }

        return returnMe;
    }

    /**
     * Attempts to acquire a non-instantaneous time-scale by inspecting the <code>physicalElement</code> and the 
     * <code>duration</code>. Currently accepts only one physical element and a selection of durations.
     * 
     * @param parameterCodes the parameter codes
     * @param source the source data to help with messaging
     * @return a non-instantaneous time-scale
     * @throws UnsupportedOperationException if the scale encoding is unsupported
     */

    private static TimeScaleOuter getNonInstantaneousTimeScale( ParameterCodes parameterCodes, URI source )
    {
        Objects.requireNonNull( parameterCodes.getPhysicalElement(),
                                "Cannot determine the time scale of the time-series in '"
                                                                     + source
                                                                     + "' because no "
                                                                     + "physicalElement parameter code is defined." );

        String physicalElement = parameterCodes.getPhysicalElement();

        if ( !"QR".equals( physicalElement ) )
        {
            // #76062-29
            throw new UnsupportedOperationException( "While attempting to deserialize a WRDS json response contained "
                                                     + "in "
                                                     + source
                                                     + ", found an unsupported physical element code '"
                                                     + physicalElement
                                                     + "'." );
        }

        // Linked to the accepted physicalElement code above
        TimeScaleFunction function = TimeScaleFunction.MEAN;

        Duration duration =
                TimeScaleFromParameterCodes.getDurationFromDurationCode( parameterCodes.getDuration(), source );

        TimeScaleOuter timeScale = TimeScaleOuter.of( duration, function );

        LOGGER.trace( "{}{}{}{}.",
                      "While processing the time-series response in ",
                      source,
                      ", discovered a time scale of ",
                      timeScale );

        return timeScale;
    }

    /**
     * Attempts to acquire a {@link Duration} from the {@link ParameterCodes#getDuration()}, which uses the SHEF 
     * encoding. See <a href="https://www.weather.gov/media/mdl/SHEF_CodeManual_5July2012.pdf">
     * https://www.weather.gov/media/mdl/SHEF_CodeManual_5July2012.pdf</a>
     * 
     * @param durationCode the duration code
     * @param source the source data to help with messaging
     * @return a duration
     * @throws UnsupportedOperationException if the duration is unsupported
     * @throws NullPointerException if the duration is null
     */

    private static Duration getDurationFromDurationCode( String durationCode, URI source )
    {
        Objects.requireNonNull( durationCode,
                                "Cannot determine the time scale of the time-series in '"
                                              + source
                                              + "' because no duration parameter code is "
                                              + "defined." );

        switch ( durationCode )
        {
            case "U":
                return Duration.ofMinutes( 1 );
            case "E":
                return Duration.ofMinutes( 5 );
            case "G":
                return Duration.ofMinutes( 10 );
            case "C":
                return Duration.ofMinutes( 15 );
            case "J":
                return Duration.ofMinutes( 30 );
            case "H":
                return Duration.ofHours( 1 );
            case "B":
                return Duration.ofHours( 2 );
            case "T":
                return Duration.ofHours( 3 );
            case "F":
                return Duration.ofHours( 4 );
            case "Q":
                return Duration.ofHours( 6 );
            case "A":
                return Duration.ofHours( 8 );
            case "K":
                return Duration.ofHours( 12 );
            case "L":
                return Duration.ofHours( 18 );
            case "D":
                return Duration.ofDays( 1 );
            default:
                throw new UnsupportedOperationException( "While attempting to deserialize a WRDS json response "
                                                         + "contained in "
                                                         + source
                                                         + ", found an unsupported duration code '"
                                                         + durationCode
                                                         + "'." );
        }

    }


}
