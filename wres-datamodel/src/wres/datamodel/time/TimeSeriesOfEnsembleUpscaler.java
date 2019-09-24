package wres.datamodel.time;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import wres.datamodel.Ensemble;
import wres.datamodel.scale.TimeScale;
import wres.datamodel.scale.ScaleValidationEvent;

/**
 * <p>Upscales each trace within an ensemble time-series using an atomic upscaler for the trace values.
 * 
 * @author james.brown@hydrosolved.com
 */

public class TimeSeriesOfEnsembleUpscaler implements TimeSeriesUpscaler<Ensemble>
{

    /**
     * List of validation events encountered when attempting to upscale the last time-series provided.
     */

    private final List<ScaleValidationEvent> validationEvents = new ArrayList<>();

    /**
     * Atomic upscaler.
     */

    private final TimeSeriesUpscaler<Double> upscaler;

    /**
     * Creates an instance with a default upscaler for the individual traces.
     * 
     * @return an instance of the ensemble upscaler
     */

    public static TimeSeriesOfEnsembleUpscaler of()
    {
        return new TimeSeriesOfEnsembleUpscaler( TimeSeriesOfDoubleBasicUpscaler.of() );
    }

    /**
     * Creates an instance with a default upscaler for the individual traces.
     * 
     * @param upscaler the upscaler to use for the traces
     * @return an instance of the ensemble upscaler
     * @throws NullPointerException if the upscaler is null
     */

    public static TimeSeriesOfEnsembleUpscaler of( TimeSeriesUpscaler<Double> upscaler )
    {
        return new TimeSeriesOfEnsembleUpscaler( upscaler );
    }

    @Override
    public TimeSeries<Ensemble> upscale( TimeSeries<Ensemble> timeSeries,
                                         TimeScale desiredTimeScale )
    {
        return this.upscale( timeSeries, desiredTimeScale, Collections.emptySet() );
    }

    @Override
    public TimeSeries<Ensemble> upscale( TimeSeries<Ensemble> timeSeries,
                                         TimeScale desiredTimeScale,
                                         Set<Instant> endsAt )
    {
        Objects.requireNonNull( timeSeries );

        Objects.requireNonNull( desiredTimeScale );

        Objects.requireNonNull( endsAt );

        // Decompose
        List<TimeSeries<Double>> traces = TimeSeriesSlicer.decompose( timeSeries );

        // Upscale each trace separately and then recompose
        List<TimeSeries<Double>> upscaled = new ArrayList<>();

        for ( TimeSeries<Double> next : traces )
        {
            upscaled.add( this.getUpscaler().upscale( next, desiredTimeScale, endsAt ) );

            // Only retain the unique validation events
            Set<ScaleValidationEvent> uniqueEvents = new HashSet<>( this.getScaleValidationEvents() );
            uniqueEvents.addAll( this.getUpscaler().getScaleValidationEvents() );
            this.validationEvents.clear();
            this.validationEvents.addAll( uniqueEvents );
        }

        // Get the labels
        SortedSet<String> labels = new TreeSet<>();

        if ( Objects.nonNull( timeSeries.getEvents().first() ) )
        {
            Optional<String[]> labs = timeSeries.getEvents().first().getValue().getLabels();
            if ( labs.isPresent() )
            {
                labels.addAll( Arrays.stream( labs.get() ).collect( Collectors.toSet() ) );
            }
        }

        return TimeSeriesSlicer.compose( Collections.unmodifiableList( upscaled ), labels );
    }

    @Override
    public List<ScaleValidationEvent> getScaleValidationEvents()
    {
        return Collections.unmodifiableList( this.validationEvents );
    }

    /**
     * Returns the trace upscaler used by this instance.
     * 
     * @return the trace upscaler
     */

    private TimeSeriesUpscaler<Double> getUpscaler()
    {
        return this.upscaler;
    }

    /**
     * Hidden constructor.
     * 
     * @param upscaler the upscaler instance
     * @throws NullPointerException of the input is null
     */

    private TimeSeriesOfEnsembleUpscaler( TimeSeriesUpscaler<Double> upscaler )
    {
        Objects.requireNonNull( upscaler );

        this.upscaler = upscaler;
    }

}
