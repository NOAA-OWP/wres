package wres.datamodel.baselines;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.components.GeneratedBaseline;
import wres.config.components.GeneratedBaselineBuilder;
import wres.datamodel.MissingValues;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.Feature;
import wres.datamodel.time.Event;
import wres.datamodel.types.Ensemble;
import wres.datamodel.time.RescaledTimeSeriesPlusValidation;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeries.Builder;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.datamodel.time.TimeSeriesSlicer;
import wres.datamodel.time.TimeSeriesUpscaler;
import wres.statistics.generated.ReferenceTime.ReferenceTimeType;
import wres.statistics.generated.TimeScale;

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
    private final Map<Feature, ClimatologyStructure> climatologySource;

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
     * @param upscaler the upscaler to use when the template series has a larger timescale than the source data
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
     * @param upscaler the upscaler to use when the template series has a larger timescale than the source data
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

        // Upscale?
        TimeScaleOuter desiredTimeScale = template.getTimeScale();
        if ( Objects.nonNull( desiredTimeScale )
             && Objects.nonNull( this.getSourceTimeScale() )
             && TimeScaleOuter.isRescalingRequired( this.getSourceTimeScale(), desiredTimeScale ) )
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

        if ( template.getEvents()
                     .isEmpty() )
        {
            LOGGER.trace( WHILE_GENERATING_A_CLIMATOLOGY_TIME_SERIES_USING_INPUT_SERIES,
                          template.hashCode(),
                          ", discovered that the input series had no events (i.e., was empty). Returning the empty "
                          + "time-series." );

            // Adjust the template metadata to use source units
            String sourceUnit = this.getClimatologySourceForTemplate( template )
                                    .metadata()
                                    .getUnit();
            TimeSeriesMetadata adjusted = this.getAdjustedMetadata( template.getMetadata(), sourceUnit );

            return TimeSeries.of( adjusted );
        }

        ClimatologyStructure structure = this.getClimatologySourceForTemplate( template );
        SortedMap<Instant, Event<Double>> eventsToSearch = structure.structure();

        // Find the superset of times to search
        int start = eventsToSearch.firstKey()
                                  .atZone( ZONE_ID )
                                  .getYear();
        int stop = eventsToSearch.lastKey()
                                 .atZone( ZONE_ID )
                                 .getYear() + 1;  // Render upper bound inclusive
        Set<Integer> years = IntStream.range( start, stop )
                                      .boxed()
                                      .collect( Collectors.toSet() );

        // Adjust the template metadata to use source units
        String sourceUnit = structure.metadata()
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
                if ( year != time.getYear()
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
        // Do not optimize for empty time-series as upscaling can change the measurement units: #121751

        LOGGER.trace( "Generating climatology for a time-series where upscaling is required." );

        // Identify the valid times for which upscaled values are required
        ClimatologyStructure source = this.getClimatologySourceForTemplate( template );
        SortedMap<Instant, Event<Double>> structure = source.structure();

        // Identify the superset of years with climatology values
        int start = structure.firstKey()
                             .atZone( ZONE_ID )
                             .getYear();
        int stop = structure.lastKey()
                            .atZone( ZONE_ID )
                            .getYear() + 1;  // Render upper bound inclusive
        Set<Integer> years = IntStream.range( start, stop )
                                      .boxed()
                                      .collect( Collectors.toSet() );

        // Iterate through the template events and add a distinct year for each one
        SortedSet<Instant> targetTimes = new TreeSet<>();
        Map<Instant, SortedSet<Instant>> ensembleTimes = new TreeMap<>();
        for ( Event<?> nextEvent : template.getEvents() )
        {
            Instant nextTime = nextEvent.getTime();
            ZonedDateTime time = nextTime.atZone( ZONE_ID );
            SortedSet<Instant> nextEnsemble = new TreeSet<>();

            // One event per year of record, at most
            for ( int year : years )
            {
                Instant targetTime = time.withYear( year )
                                         .toInstant();

                // Skip the source event at the same time, aka verifying observation
                if ( year != time.getYear()
                     && this.isAdmissable( targetTime ) )
                {
                    targetTimes.add( targetTime );
                    nextEnsemble.add( targetTime );
                }
            }
            ensembleTimes.put( nextTime, nextEnsemble );
        }

        // If the template series has an unknown timescale function, assume it is mean
        TimeScaleOuter desiredTimeScale = template.getTimeScale();
        if ( desiredTimeScale.getFunction() == TimeScale.TimeScaleFunction.UNKNOWN )
        {
            TimeScale adjusted = desiredTimeScale.getTimeScale()
                                                 .toBuilder()
                                                 .setFunction( TimeScale.TimeScaleFunction.MEAN )
                                                 .build();
            desiredTimeScale = TimeScaleOuter.of( adjusted );
            if ( LOGGER.isTraceEnabled() )
            {
                LOGGER.trace( "When creating an upscaled climatology time-series from a template time-series, "
                              + "encountered a template time-series with UNKNOWN timescale function. Assuming that the "
                              + "timescale function is MEAN." );
            }
        }

        // Rescale
        RescaledTimeSeriesPlusValidation<Double> rescaled = this.upscaler.upscale( source.timeSeries(),
                                                                                   desiredTimeScale,
                                                                                   targetTimes,
                                                                                   this.desiredUnit );

        RescaledTimeSeriesPlusValidation.logScaleValidationWarnings( source.timeSeries(),
                                                                     rescaled.getValidationEvents() );

        TimeSeries<Double> rescaledSeries = rescaled.getTimeSeries();

        return this.getEnsembleSeriesFromRescaledSeries( template.getMetadata(),
                                                         rescaledSeries,
                                                         ensembleTimes );
    }

    /**
     * Generates an ensemble time-series from a rescaled single-valued time-series that contains all the rescaled
     * events at each valid time.
     * @param templateMetadata the metadata of the template time-series
     * @param rescaledSeries the rescaled time-series
     * @param ensembleTimes the times of the ensemble events to compose, with one collection per ensemble valid time
     * @return the ensemble time-series
     */

    private TimeSeries<Ensemble> getEnsembleSeriesFromRescaledSeries( TimeSeriesMetadata templateMetadata,
                                                                      TimeSeries<Double> rescaledSeries,
                                                                      Map<Instant, SortedSet<Instant>> ensembleTimes )
    {

        // Adjust the template metadata to use source units
        String sourceUnit = rescaledSeries.getMetadata()
                                          .getUnit();
        TimeSeriesMetadata adjusted = this.getAdjustedMetadata( templateMetadata, sourceUnit );
        TimeSeries.Builder<Ensemble> builder = new TimeSeries.Builder<Ensemble>().setMetadata( adjusted );

        SortedSet<Event<Double>> rescaledEvents = rescaledSeries.getEvents();
        // Map the rescaled events by time
        Map<Instant, Event<Double>> eventsByTime = rescaledEvents.stream()
                                                                 .collect( Collectors.toMap( Event::getTime,
                                                                                             Function.identity() ) );

        if ( !rescaledEvents.isEmpty() )
        {
            for ( Map.Entry<Instant, SortedSet<Instant>> nextEnsemble : ensembleTimes.entrySet() )
            {
                Instant nextTime = nextEnsemble.getKey();
                SortedSet<Instant> nextEnsembleTimes = nextEnsemble.getValue();
                String[] labelStrings = new String[nextEnsembleTimes.size()];
                double[] members = new double[labelStrings.length];
                int count = 0;
                for ( Instant nextEnsembleTime : nextEnsembleTimes )
                {
                    ZonedDateTime time = nextEnsembleTime.atZone( ZONE_ID );
                    int year = time.getYear();
                    String label = Integer.toString( year );
                    labelStrings[count] = label;
                    double member = MissingValues.DOUBLE;

                    if ( eventsByTime.containsKey( nextEnsembleTime ) )
                    {
                        Event<Double> event = eventsByTime.get( nextEnsembleTime );
                        member = event.getValue();
                    }
                    members[count] = member;
                    count++;
                }

                Ensemble.Labels labels = Ensemble.Labels.of( labelStrings );
                Ensemble ensemble = Ensemble.of( members, labels );
                Event<Ensemble> ensembleEvent = Event.of( nextTime, ensemble );
                builder.addEvent( ensembleEvent );
            }
        }

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

    private ClimatologyStructure getClimatologySourceForTemplate( TimeSeries<?> template )
    {
        // Feature correlation assumes that the template feature is right-ish and the source feature is baseline-ish
        // If this is no longer a safe assumption, then the orientation should be declared on construction
        Feature templateFeature = template.getMetadata()
                                          .getFeature();

        if ( !this.climatologySource.containsKey( templateFeature ) )
        {
            Set<Feature> sourceFeatures = this.climatologySource.values()
                                                                .stream()
                                                                .map( next -> next.metadata()
                                                                                  .getFeature() )
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

    /**
     * Hidden constructor.
     *
     * @param climatologySource the source data for the climatology values, not null
     * @param upscaler the upscaler to use when the template series has a larger timescale than the source data
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
        Map<Feature, ClimatologyStructure> consolidated = new HashMap<>();
        for ( Map.Entry<Feature, List<TimeSeries<Double>>> nextEntry : grouped.entrySet() )
        {
            Feature nextFeature = nextEntry.getKey();
            List<TimeSeries<Double>> series = nextEntry.getValue();
            TimeSeries<Double> nextConsolidated = this.consolidate( series, nextFeature.getName() );
            SortedMap<Instant, Event<Double>> eventsToSearch =
                    nextConsolidated.getEvents()
                                    .stream()
                                    .collect( Collectors.toMap( Event::getTime,
                                                                Function.identity(),
                                                                ( a, b ) -> a, // Cannot be dups in TimeSeries
                                                                TreeMap::new ) );
            ClimatologyStructure structure = new ClimatologyStructure( nextConsolidated.getMetadata(),
                                                                       eventsToSearch,
                                                                       nextConsolidated );
            consolidated.put( nextFeature, structure );
        }

        this.climatologySource = Collections.unmodifiableMap( consolidated );
        this.upscaler = upscaler;
        this.desiredUnit = desiredUnit;
        this.climatologySourceMetadata = this.climatologySource.values()
                                                               .stream()
                                                               .findAny()
                                                               .orElseThrow() // Already checked above that it exists
                                                               .metadata();

        LOGGER.debug( "Created a climatology generator." );
    }

    /**
     * A structure to assist with building a climatological dataset.
     * @param metadata the metadata
     * @param structure the structure
     * @param timeSeries the time-series whose structure is represented
     */

    private record ClimatologyStructure( TimeSeriesMetadata metadata,
                                         SortedMap<Instant, Event<Double>> structure,
                                         TimeSeries<Double> timeSeries )
    {
    }

}
