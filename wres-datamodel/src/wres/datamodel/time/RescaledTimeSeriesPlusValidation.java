package wres.datamodel.time;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import wres.datamodel.scale.ScaleValidationEvent;

/**
 * An immutable value class that stores the results of a rescaling operation, namely:
 * 
 * <ol>
 * <li>The rescaled time-series; and</li>
 * <li>Any validation events encountered when rescaling, such as warnings.</li>
 * </ol>
 * 
 * @author james.brown@hydrosolved.com
 * @param <T> the type of event values in the time-series
 */

public class RescaledTimeSeriesPlusValidation<T>
{

    /**
     * The rescaled time-series.
     */

    private final TimeSeries<T> timeSeries;

    /**
     * The scale validation events.
     */

    private final List<ScaleValidationEvent> validationEvents;

    /**
     * Returns an instance.
     * 
     * @param timeSeries the time-series
     * @param validationEvents the validation events
     * @throws NullPointerException if either input is null
     */

    static <T> RescaledTimeSeriesPlusValidation<T> of( TimeSeries<T> timeSeries,
                                                       List<ScaleValidationEvent> validationEvents )
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
        return timeSeries;
    }

    /**
     * Returns the validation events associated with the rescaled time-series.
     * 
     * @return the validation events
     */

    public List<ScaleValidationEvent> getValidationEvents()
    {
        return validationEvents; // Rendered immutable on construction
    }

    /**
     * Returns an instance.
     * 
     * @param timeSeries the time-series
     * @param validationEvents the validation events
     * @throws NullPointerException if either input is null
     */

    private RescaledTimeSeriesPlusValidation( TimeSeries<T> timeSeries, List<ScaleValidationEvent> validationEvents )
    {
        Objects.requireNonNull( timeSeries );

        Objects.requireNonNull( validationEvents );

        this.timeSeries = timeSeries;
        this.validationEvents = Collections.unmodifiableList( validationEvents );
    }

}