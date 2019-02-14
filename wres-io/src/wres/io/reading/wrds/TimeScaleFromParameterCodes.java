package wres.io.reading.wrds;

import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.metadata.TimeScale;

/**
 * <p>Helper class that builds a {@link TimeScale} from a set of {@link ParameterCodes}.
 * A {@link TimeScale} comprises a {@link TimeScale#getPeriod()} and a 
 * {@link TimeScale#getFunction()}. The main hint is the 
 * {@link ParameterCodes#getDuration()}, which describes the <code>period</code> 
 * over which the value applies. The <code>function</code> depends on the 
 * {@link ParameterCodes#getPhysicalElement()}. See #60158-11. 
 * 
 * <p>Importantly, this class is lenient when interpreting the time scale
 * information and will log a warning and return a <code>null</code> 
 * time scale when the time scale cannot be determined.
 * 
 * TODO: implement this class more fully, accounting for time scales other
 * than instantaneous
 * 
 * @author james.brown@hydrosolved.com
 */

class TimeScaleFromParameterCodes
{

    /**
     * Logger.
     */
    
    private static final Logger LOGGER = LoggerFactory.getLogger( TimeScaleFromParameterCodes.class );
    
    /**
     * Returns a {@link TimeScale} from the input parameter codes.
     * 
     * @param parameterCodes the parameter codes
     * @return a time scale or null if one could not be interpreted
     * @throws NullPointerException if the inputs is null
     */

    static TimeScale getTimeScale( ParameterCodes parameterCodes )
    {
        Objects.requireNonNull( parameterCodes,
                                "Specify non-null parameter codes for a WRDS source." );

        TimeScale returnMe = null;
        
        if ( "I".equalsIgnoreCase( parameterCodes.getDuration() ) )
        {
            returnMe = TimeScale.of();
        }
        else
        {
            LOGGER.warn( "Cannot determine the time scale of the WRDS source "
                    + "from the input parameter codes '{}'.", parameterCodes );
        }

        return returnMe;
    }

}
