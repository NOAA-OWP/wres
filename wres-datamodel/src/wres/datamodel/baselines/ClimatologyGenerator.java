package wres.datamodel.baselines;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.yaml.components.GeneratedBaseline;
import wres.config.yaml.components.GeneratedBaselineBuilder;
import wres.datamodel.messages.EvaluationStatusMessage;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.Feature;
import wres.datamodel.time.Event;
import wres.datamodel.Ensemble;
import wres.datamodel.time.RescaledTimeSeriesPlusValidation;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeries.Builder;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.datamodel.time.TimeSeriesSlicer;
import wres.datamodel.time.TimeSeriesUpscaler;
import wres.statistics.generated.ReferenceTime.ReferenceTimeType;

/**
 * <p>Generates a climatological time-series from a source of climatological data supplied on construction. The shape
 * of the climatological series is obtained from a template time-series supplied on demand. The template time-series
 * must use the same feature identity as the data source from which the climatological time-series is generated. This
 * class generates time-series that are analogous to the U.S. National Weather Service's Ensemble Streamflow Prediction
 * (ESP) forecasts. The verifying observation is not contained within the forecast when producing a forecast whose
 * valid dates span the historical record, i.e., a "reforecast" or "hindcast".
 *
 * @author James Brown
 */

public class ClimatologyGenerator implements BaselineGenerator<Ensemble>
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( ClimatologyGenerator.class );

    /** Re-used string. */
    private static final String WHILE_GENERATING_A_CLIMATOLOGY_TIME_SERIES_USING_INPUT_SERIES =
            "While generating a climatology time-series using input series {}{}";

    /** The source data from which the climatology values should be generated, indexed by feature. */
    private final Map<Feature, TimeSeries<Double>> climatologySource;

    /** Representative time-series metadata from the climatology source. */
    private final TimeSeriesMetadata climatologySourceMetadata;

    /** Zone ID, used several times. */
    private static final ZoneId ZONE_ID = ZoneId.of( "UTC" );

    /** An optional upscaler to use in generating a climatology value from the {@link #climatologySource}. */
    private final TimeSeriesUpscaler<Double> upscaler;

    /** The desired measurement unit. **/
    private final String desiredUnit;

    /** Start date for the climatological data. */
    private final Instant minimum;

    /** End date for the climatological data. */
    private final Instant maximum;

    /**
     * Provides an instance.
     *
     * @param climatologySource the climatology data source, required
     * @param upscaler the temporal upscaler, which is required if the template series has a larger scale than the
     *                 climatologySource
     * @param desiredUnit the desired measurement unit, required
     * @return an instance
     * @throws NullPointerException if any required input is null
     */

    public static ClimatologyGenerator of( Supplier<Stream<TimeSeries<Double>>> climatologySource,
                                           TimeSeriesUpscaler<Double> upscaler,
                                           String desiredUnit )
    {
        return ClimatologyGenerator.of( climatologySource,
                                        upscaler,
                                        desiredUnit,
                                        // Default parameters
                                        GeneratedBaselineBuilder.builder()
                                                                .build() );
    }

    /**
     * Provides an instance.
     *
     * @param climatologySource the climatology data source, required
     * @param upscaler the temporal upscaler, which is required if the template series has a larger scale than the
     *                 climatologySource
     * @param desiredUnit the desired measurement unit, required
     * @param parameters the parameters
     * @return an instance
     * @throws NullPointerException if any required input is null
     * @throws BaselineGeneratorException if the baseline generator could not be created for any other reason
     */

    public static ClimatologyGenerator of( Supplier<Stream<TimeSeries<Double>>> climatologySource,
                                           TimeSeriesUpscaler<Double> upscaler,
                                           String desiredUnit,
                                           GeneratedBaseline parameters )
    {
        return new ClimatologyGenerator( climatologySource,
                                         upscaler,
                                         desiredUnit,
                                         parameters );
    }

    /**
     * Creates a climatology time-series at a lag supplied on construction using the input time-series as a template.
     *
     * @param template the template time-series for which climatology values will be generated
     * @return a time-series with the lagged value at every time-step
     * @throws NullPointerException if the input is null
     * @throws BaselineGeneratorException if the climatology series could not be generated for any reason
     */

    @Override
    public TimeSeries<Ensemble> apply( TimeSeries<?> template )
    {
        Objects.requireNonNull( template );

        if ( template.getEvents()
                     .isEmpty() )
        {
            LOGGER.trace( WHILE_GENERATING_A_CLIMATOLOGY_TIME_SERIES_USING_INPUT_SERIES,
                          template.hashCode(),
                          ", discovered that the input series had no events (i.e., was empty). Returning the empty "
                          + "time-series." );

            // Adjust the template metadata to use source units
            TimeSeries<Double> source = this.getClimatologySourceForTemplate( template );
            String sourceUnit = source.getMetadata()
                                      .getUnit();
            TimeSeriesMetadata adjusted = this.getAdjustedMetadata( template.getMetadata(), sourceUnit );

            return TimeSeries.of( adjusted );
        }

        // Upscale?
        TimeScaleOuter desiredTimeScale = template.getTimeScale();
        if ( Objects.nonNull( desiredTimeScale )
             && Objects.nonNull( this.getSourceTimeScale() )
             && !desiredTimeScale.equals( this.getSourceTimeScale() ) )
        {
            return this.getClimatologyWithUpscaling( template );
        }
        else
        {
            return this.getClimatologyWithoutUpscaling( template );
        }
    }

    /**
     * Returns a climatology time-series when no upscaling is required.
     * @param template the template time-series
     * @return a climatology time-series
     */

    private TimeSeries<Ensemble> getClimatologyWithoutUpscaling( TimeSeries<?> template )
    {
        LOGGER.trace( "Generating climatology for a time-series where upscaling is not required." );

        TimeSeries<Double> source = this.getClimatologySourceForTemplate( template );
        Map<Instant, Event<Double>> eventsToSearch = source.getEvents()
                                                           .stream()
                                                           .collect( Collectors.toMap( Event::getTime,
                                                                                       Function.identity() ) );

        // Identify the years with climatology values
        Set<Integer> years = eventsToSearch.keySet()
                                           .stream()
                                           .map( n -> n.atZone( ZONE_ID ) )
                                           .map( n -> n.get( ChronoField.YEAR ) )
                                           .collect( Collectors.toSet() );

        // Adjust the template metadata to use source units
        String sourceUnit = source.getMetadata()
                                  .getUnit();
        TimeSeriesMetadata adjusted = this.getAdjustedMetadata( template.getMetadata(), sourceUnit );
        TimeSeries.Builder<Ensemble> builder = new TimeSeries.Builder<Ensemble>().setMetadata( adjusted );

        // Iterate through the template events and find corresponding climatology events
        for ( Event<?> nextEvent : template.getEvents() )
        {
            Instant nextTime = nextEvent.getTime();
            ZonedDateTime time = nextTime.atZone( ZONE_ID );

            String[] labelStrings = new String[years.size()];
            double[] members = new double[labelStrings.length];
            int count = 0;

            // One event per year of record, at most
            for ( int year : years )
            {
                Instant targetTime = time.withYear( year )
                                         .toInstant();
                // Skip the source event at the same time, aka verifying observation
                if ( year != time.get( ChronoField.YEAR )
                     && this.isAdmissable( targetTime ) )
                {
                    Event<Double> targetEvent = eventsToSearch.get( targetTime );

                    if ( Objects.nonNull( targetEvent ) )
                    {
                        labelStrings[count] = year + "";
                        members[count] = targetEvent.getValue();
                        count++;
                    }
                }
            }

            // Adjust the size for the events discovered
            if ( count < labelStrings.length )
            {
                labelStrings = Arrays.copyOfRange( labelStrings, 0, count );
                members = Arrays.copyOfRange( members, 0, count );
            }

            Ensemble.Labels labels = Ensemble.Labels.of( labelStrings );
            Ensemble ensemble = Ensemble.of( members, labels );
            Event<Ensemble> ensembleEvent = Event.of( nextTime, ensemble );
            builder.addEvent( ensembleEvent );
        }

        return builder.build();
    }

    /**
     * Returns a climatology time-series when upscaling is required.
     * @param template the template time-series
     * @return a climatology time-series
     */

    private TimeSeries<Ensemble> getClimatologyWithUpscaling( TimeSeries<?> template )
    {
        LOGGER.trace( "Generating climatology for a time-series where upscaling is required." );

        // Identify the valid times for which upscaled values are required
        TimeSeries<Double> source = this.getClimatologySourceForTemplate( template );
        Set<Integer> years = source.getEvents()
                                   .stream()
                                   .map( Event::getTime )
                                   .map( n -> n.atZone( ZONE_ID ) )
                                   .map( n -> n.get( ChronoField.YEAR ) )
                                   .collect( Collectors.toSet() );

        // Adjust the template metadata to use source units
        String sourceUnit = source.getMetadata()
                                  .getUnit();
        TimeSeriesMetadata adjusted = this.getAdjustedMetadata( template.getMetadata(), sourceUnit );
        TimeSeries.Builder<Ensemble> builder = new TimeSeries.Builder<Ensemble>().setMetadata( adjusted );
        List<EvaluationStatusMessage> scaleWarnings = new ArrayList<>();

        // Iterate through the template events and create an upscaled climatology event
        for ( Event<?> nextEvent : template.getEvents() )
        {
            Instant nextTime = nextEvent.getTime();
            ZonedDateTime time = nextTime.atZone( ZONE_ID );

            String[] labelStrings = new String[years.size()];
            double[] members = new double[labelStrings.length];
            int count = 0;

            // One event per year of record, at most
            for ( int year : years )
            {
                Instant targetTime = time.withYear( year )
                                         .toInstant();

                // Skip the source event at the same time, aka verifying observation
                if ( year != time.get( ChronoField.YEAR )
                     && this.isAdmissable( targetTime ) )
                {
                    RescaledTimeSeriesPlusValidation<Double> rescaled = this.upscaler.upscale( source,
                                                                                               template.getTimeScale(),
                                                                                               new TreeSet<>( Set.of(
                                                                                                       targetTime ) ),
                                                                                               this.desiredUnit );
                    scaleWarnings.addAll( rescaled.getValidationEvents() );
                    TimeSeries<Double> rescaledSeries = rescaled.getTimeSeries();
                    Event<Double> rescaledEvent = rescaledSeries.getEvents()
                                                                .first();

                    if ( Objects.nonNull( rescaledEvent ) )
                    {
                        labelStrings[count] = year + "";
                        members[count] = rescaledEvent.getValue();
                        count++;
                    }
                }
            }

            // Adjust the size for the events discovered
            if ( count < labelStrings.length )
            {
                labelStrings = Arrays.copyOfRange( labelStrings, 0, count );
                members = Arrays.copyOfRange( members, 0, count );
            }

            Ensemble.Labels labels = Ensemble.Labels.of( labelStrings );
            Ensemble ensemble = Ensemble.of( members, labels );
            Event<Ensemble> ensembleEvent = Event.of( nextTime, ensemble );
            builder.addEvent( ensembleEvent );
        }

        RescaledTimeSeriesPlusValidation.logScaleValidationWarnings( source, scaleWarnings );

        return builder.build();
    }

    /**
     * @param targetTime the time to check for containment
     * @return whether the target time is within the overall interval of allowed times
     */

    private boolean isAdmissable( Instant targetTime )
    {
        return !targetTime.isBefore( this.minimum )
               && !targetTime.isAfter( this.maximum );
    }

    /**
     * Adjusts the supplied metadata to use the measurement unit associated with the source time-series from which the
     * climatology is generated.
     * @param metadata the metadata to adjust
     * @param sourceUnit the source measurement unit
     * @return the adjusted metadata
     */

    private TimeSeriesMetadata getAdjustedMetadata( TimeSeriesMetadata metadata,
                                                    String sourceUnit )
    {
        return metadata.toBuilder()
                       .setUnit( sourceUnit )
                       .build();
    }

    /**
     * @return the timescale of the climatology source
     */

    private TimeScaleOuter getSourceTimeScale()
    {
        return this.climatologySourceMetadata.getTimeScale();
    }

    /**
     * @return the time-series from the climatology source whose feature name matches the template series feature name
     * @throws BaselineGeneratorException if the template feature does not match a feature for which source data exists
     */

    private TimeSeries<Double> getClimatologySourceForTemplate( TimeSeries<?> template )
    {
        // Feature correlation assumes that the template feature is right-ish and the source feature is baseline-ish
        // If this is no longer a safe assumption, then the orientation should be declared on construction
        Feature templateFeature = template.getMetadata()
                                          .getFeature();

        if ( !this.climatologySource.containsKey( templateFeature ) )
        {
            Set<Feature> sourceFeatures = this.climatologySource.values()
                                                                .stream()
                                                                .map( next -> next.getMetadata().getFeature() )
                                                                .collect( Collectors.toSet() );

            throw new BaselineGeneratorException( "When building a climatology baseline, failed to discover a source "
                                                  + "time-series for the template time-series with feature: "
                                                  + templateFeature
                                                  + ". Source time-series were only available for features: "
                                                  + sourceFeatures
                                                  + "." );
        }

        return this.climatologySource.get( templateFeature );
    }

    /**
     * Hidden constructor.
     *
     * @param climatologySource the source data for the climatology values, not null
     * @param upscaler the temporal upscaler, which is required if the template series has a larger scale than the
     *                 climatologySource
     * @param desiredUnit the desired measurement unit, not null
     * @param parameters the parameters
     * @throws NullPointerException if any required input is null
     * @throws BaselineGeneratorException if the generator could not be created for any other reason
     */

    private ClimatologyGenerator( Supplier<Stream<TimeSeries<Double>>> climatologySource,
                                  TimeSeriesUpscaler<Double> upscaler,
                                  String desiredUnit,
                                  GeneratedBaseline parameters )
    {
        Objects.requireNonNull( parameters );
        Objects.requireNonNull( climatologySource );
        Objects.requireNonNull( desiredUnit );

        this.minimum = parameters.minimumDate();
        this.maximum = parameters.maximumDate();

        if ( Objects.nonNull( this.minimum )
             && Objects.nonNull( this.maximum )
             && !this.maximum.isAfter( this.minimum ) )
        {
            throw new BaselineGeneratorException( "The climatology period is invalid. The 'maximum' date must be later "
                                                  + "than the 'minimum' date, but the 'maximum' date is "
                                                  + this.maximum
                                                  + " and the 'minimum' date is "
                                                  + this.minimum
                                                  + ". Please declare an "
                                                  + "earlier 'minimum' or a later 'maximum' and try again." );
        }

        LOGGER.debug( "When generating climatology, using a minimum date of {} and a maximum date of {}.",
                      this.minimum,
                      this.maximum );

        // Retrieve the time-series on construction
        List<TimeSeries<Double>> source = climatologySource.get()
                                                           .toList();

        // The climatology source cannot be empty
        if ( source.isEmpty()
             || source.stream()
                      .mapToInt( n -> n.getEvents().size() )
                      .sum() == 0 )
        {
            throw new BaselineGeneratorException( "Cannot create a climatology time-series without one or more source "
                                                  + "time-series that contain some events." );
        }

        // The climatology source cannot contain forecast-like time-series
        if ( source.stream()
                   .anyMatch( next -> next.getReferenceTimes()
                                          .containsKey( ReferenceTimeType.T0 ) ) )
        {
            throw new BaselineGeneratorException( "When attempting to generate a climatology baseline, discovered "
                                                  + "one or more time-series that contained a reference time with "
                                                  + "type 'T0', which is indicative of a forecast. Forecast-like "
                                                  + "time-series are not valid as the data source for a climatology "
                                                  + "baseline. Instead, declare observation-like time-series as the "
                                                  + "baseline data source." );
        }

        // Perform the consolidation all at once rather than for each pair of time-series. See #111801
        Map<Feature, List<TimeSeries<Double>>> grouped = source.stream()
                                                               .collect( Collectors.groupingBy( next -> next.getMetadata()
                                                                                                            .getFeature(),
                                                                                                Collectors.mapping(
                                                                                                        Function.identity(),
                                                                                                        Collectors.toList() ) ) );

        // Consolidate the time-series by feature
        Map<Feature, TimeSeries<Double>> consolidated = new HashMap<>();
        for ( Map.Entry<Feature, List<TimeSeries<Double>>> nextEntry : grouped.entrySet() )
        {
            Feature nextFeature = nextEntry.getKey();
            List<TimeSeries<Double>> series = nextEntry.getValue();
            TimeSeries<Double> nextConsolidated = this.consolidate( series, nextFeature.getName() );
            consolidated.put( nextFeature, nextConsolidated );
        }

        this.climatologySource = Collections.unmodifiableMap( consolidated );
        this.upscaler = upscaler;
        this.desiredUnit = desiredUnit;
        this.climatologySourceMetadata = this.climatologySource.values()
                                                               .stream()
                                                               .findAny()
                                                               .orElseThrow() // Already checked above that it exists
                                                               .getMetadata();

        LOGGER.debug( "Created a climatology generator." );
    }

    /**
     * Consolidate the time-series and emit a warning if duplicates are encountered. Unlike
     * {@link TimeSeriesSlicer#consolidate(Collection)}, this method admits duplicates.
     *
     * @param toConsolidate the time-series to consolidate
     * @param featureName the feature name to help with messaging
     * @return the consolidated time-series
     */

    private TimeSeries<Double> consolidate( List<TimeSeries<Double>> toConsolidate, String featureName )
    {
        SortedSet<Event<Double>> events =
                new TreeSet<>( Comparator.comparing( Event::getTime ) );

        Set<Instant> duplicates = new TreeSet<>();
        Builder<Double> builder = new Builder<>();
        for ( TimeSeries<Double> nextSeries : toConsolidate )
        {
            builder.setMetadata( nextSeries.getMetadata() );
            SortedSet<Event<Double>> nextEvents = nextSeries.getEvents();
            for ( Event<Double> nextEvent : nextEvents )
            {
                boolean added = events.add( nextEvent );
                if ( !added )
                {
                    duplicates.add( nextEvent.getTime() );
                }
            }
        }

        TimeSeries<Double> consolidated = builder.setEvents( events )
                                                 .build();

        if ( !duplicates.isEmpty() && LOGGER.isWarnEnabled() )
        {
            LOGGER.warn( "While generating a climatology baseline for feature '{}', encountered duplicate events in "
                         + "the source data at {} valid times. Using only the first event encountered at each "
                         + "duplicated time. If this is unintended, please de-duplicate the climatology data before "
                         + "creating a climatology baseline. The common time-series metadata for the time-series that "
                         + "contained duplicate events was: {}. The first duplicate time was: {}.",
                         featureName,
                         duplicates.size(),
                         consolidated.getMetadata(),
                         duplicates.iterator()
                                   .next() );
        }

        return consolidated;
    }

}
