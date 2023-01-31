package wres.datamodel.time;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.messages.EvaluationStatusMessage;
import wres.statistics.generated.EvaluationStatus.EvaluationStatusEvent.StatusLevel;

/**
 * An immutable value class that stores the results of a rescaling operation, namely:
 * 
 * <ol>
 * <li>The rescaled time-series; and</li>
 * <li>Any validation events encountered when rescaling, such as warnings.</li>
 * </ol>
 * 
 * @author James Brown
 * @param <T> the type of event values in the time-series
 */

public class RescaledTimeSeriesPlusValidation<T>
{

    /**
     * Logger.
     */

    private static final Logger LOGGER = LoggerFactory.getLogger( RescaledTimeSeriesPlusValidation.class );

    /**
     * The rescaled time-series.
     */

    private final TimeSeries<T> timeSeries;

    /**
     * The scale validation events.
     */

    private final List<EvaluationStatusMessage> validationEvents;

    /**
     * Returns an instance.
     * 
     * @param timeSeries the time-series
     * @param validationEvents the validation events
     * @throws NullPointerException if either input is null
     */

    static <T> RescaledTimeSeriesPlusValidation<T> of( TimeSeries<T> timeSeries,
                                                       List<EvaluationStatusMessage> validationEvents )
    {
        return new RescaledTimeSeriesPlusValidation<>( timeSeries, validationEvents );
    }

    /**
     * Returns the rescaled time-series.
     * 
     * @return the time series
     */

    public TimeSeries<T> getTimeSeries()
    {
        return this.timeSeries;
    }

    /**
     * Returns the validation events associated with the rescaled time-series.
     * 
     * @return the validation events
     */

    public List<EvaluationStatusMessage> getValidationEvents()
    {
        return this.validationEvents; // Rendered immutable on construction
    }

    /**
     * Logs the validation events associated with rescaling.
     * 
     * TODO: these warnings could probably be consolidated and the context information improved. May need to add 
     * more complete metadata information to the times-series.
     * 
     * @param context the context for the warnings
     * @param scaleValidationEvents the scale validation events
     */

    public static void logScaleValidationWarnings( TimeSeries<?> context,
                                                   List<EvaluationStatusMessage> scaleValidationEvents )
    {
        Objects.requireNonNull( scaleValidationEvents );

        // Any warnings? Push to log for now, but see #61930 (logging isn't for users)
        if ( LOGGER.isWarnEnabled() )
        {
            Set<EvaluationStatusMessage> warnEvents = scaleValidationEvents.stream()
                                                                           .filter( a -> a.getStatusLevel() == StatusLevel.WARN )
                                                                           .collect( Collectors.toSet() );
            if ( !warnEvents.isEmpty() )
            {
                StringJoiner message = new StringJoiner( System.lineSeparator() );
                String spacer = "    ";
                warnEvents.stream().forEach( e -> message.add( spacer + e.toString() ) );

                LOGGER.warn( "While rescaling time-series with metadata {}, encountered {} validation "
                             + "warnings, as follows: {}{}",
                             context.getMetadata(),
                             warnEvents.size(),
                             System.lineSeparator(),
                             message );
            }
        }

        // Any user-facing debug-level events? Push to log for now, but see #61930 (logging isn't for users)
        if ( LOGGER.isDebugEnabled() )
        {
            Set<EvaluationStatusMessage> debugWarnEvents = scaleValidationEvents.stream()
                                                                                .filter( a -> a.getStatusLevel() == StatusLevel.DEBUG )
                                                                                .collect( Collectors.toSet() );
            if ( !debugWarnEvents.isEmpty() )
            {
                StringJoiner message = new StringJoiner( System.lineSeparator() );
                String spacer = "    ";
                debugWarnEvents.stream().forEach( e -> message.add( spacer + e.toString() ) );

                LOGGER.debug( "While rescaling time-series with metadata {}, encountered {} detailed validation "
                              + "warnings, as follows: {}{}",
                              context.getMetadata(),
                              debugWarnEvents.size(),
                              System.lineSeparator(),
                              message );
            }
        }

    }

    /**
     * Returns an instance.
     * 
     * @param timeSeries the time-series
     * @param validationEvents the validation events
     * @throws NullPointerException if either input is null
     */

    private RescaledTimeSeriesPlusValidation( TimeSeries<T> timeSeries, List<EvaluationStatusMessage> validationEvents )
    {
        Objects.requireNonNull( timeSeries );

        Objects.requireNonNull( validationEvents );

        this.timeSeries = timeSeries;
        this.validationEvents = Collections.unmodifiableList( validationEvents );
    }

}