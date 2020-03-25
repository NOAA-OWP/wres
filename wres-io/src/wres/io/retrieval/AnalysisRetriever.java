package wres.io.retrieval;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.time.Event;
import wres.datamodel.time.ReferenceTimeType;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesSlicer;
import wres.datamodel.time.TimeWindow;

/**
 * Retrieves data from the wres.TimeSeries and wres.TimeSeriesValue tables but
 * in the pattern expected for treating the nth timestep of each analysis as if
 * it were an event in a timeseries across analyses, sort of like observations.
 *
 * The reason for separating it from forecast and observation timeseries
 * retrieval is that each analysis has N events in an actual timeseries, but the
 * structure and use of the analyses and origin of analyses differs from both
 * observation and timeseries. The structure of an NWM analysis, for example, is
 * akin to an NWM forecast, with a reference datetime and valid datetimes.
 * However, when using the analyses in an evaluation of forecasts, one event
 * from each analysis is picked out and a broader timeseries is created.
 */

class AnalysisRetriever extends TimeSeriesRetriever<Double>
{
    /**
     * Policy for handling duplicates by valid time.
     */

    enum DuplicatePolicy
    {
        /**
         * Maintain all duplicates.
         */

        KEEP_ALL,

        /**
         * Keep only one duplicate by valid time, namely the latest one by reference time.
         */

        KEEP_LATEST_REFERENCE_TIME,

        /**
         * Keep only one duplicate by valid time, namely the earliest one by reference time.
         */

        KEEP_EARLIEST_REFERENCE_TIME;
    }

    private static final Logger LOGGER = LoggerFactory.getLogger( AnalysisRetriever.class );

    private final TimeSeriesRetriever<Double> individualAnalysisRetriever;
    private final Duration earliestAnalysisDuration;
    private final Duration latestAnalysisDuration;
    private final DuplicatePolicy duplicatePolicy;

    private AnalysisRetriever( Builder builder )
    {
        super( builder, "TS.initialization_date", "TSV.lead" );
        this.earliestAnalysisDuration = builder.earliestAnalysisDuration;
        this.latestAnalysisDuration = builder.latestAnalysisDuration;
        this.duplicatePolicy = builder.duplicatePolicy;

        // Change the lead duration to the analysis step set by the user,
        // also set the reference datetime to an infinitely wide range so
        // that we do not restrict the analyses incorrectly.
        TimeWindow originalRanges = super.getTimeWindow();
        TimeWindow analysisRanges = originalRanges;

        if ( Objects.nonNull( this.getEarliestAnalysisDuration() )
             || Objects.nonNull( this.getLatestAnalysisDuration() ) )
        {
            LOGGER.debug( "Building a single-event analysis retriever for each analysis duration between {} and {}.",
                          this.getEarliestAnalysisDuration(),
                          this.getLatestAnalysisDuration() );

            // See discussion around #74987-174. Ignoring reference times when forming this selection is arbitrary. At 
            // the same time, the declaration does not provide a mechanism to clarify how specific types of reference 
            // time should be treated when that declaration is concerned with filtering or pooling by reference time.
            // TODO: be explicit about the connection between reference times/types and declaration options. 
            // For now, reference times are not used to filter here
            Instant earliestValidTime = Instant.MIN;
            Instant latestValidTime = Instant.MAX;

            if ( Objects.nonNull( originalRanges ) )
            {
                earliestValidTime = originalRanges.getEarliestValidTime();
                latestValidTime = originalRanges.getLatestValidTime();
            }

            analysisRanges = TimeWindow.of( Instant.MIN,
                                            Instant.MAX,
                                            earliestValidTime,
                                            latestValidTime,
                                            this.getEarliestAnalysisDuration(),
                                            this.getLatestAnalysisDuration() );
        }
        else
        {
            LOGGER.debug( "Building a multi-event analysis retriever." );

            // See discussion around #74987-174. Ignoring reference times when forming this selection is arbitrary. At 
            // the same time, the declaration does not provide a mechanism to clarify how specific types of reference 
            // time should be treated when that declaration is concerned with filtering or pooling by reference time.
            // TODO: be explicit about the connection between reference times/types and declaration options. 
            // For now, reference times are not used to filter here
            analysisRanges = TimeWindow.of( Instant.MIN,
                                            Instant.MAX,
                                            originalRanges.getEarliestValidTime(),
                                            originalRanges.getLatestValidTime() );
        }

        LOGGER.debug( "Using a duplicate handling policy of {} for the retrieval of analysis time-series.",
                      this.duplicatePolicy );

        this.individualAnalysisRetriever =
                new SingleValuedForecastRetriever.Builder().setDatabase( super.getDatabase() )
                                                           .setProjectId( super.getProjectId() )
                                                           .setDeclaredExistingTimeScale( super.getDeclaredExistingTimeScale() )
                                                           .setDesiredTimeScale( super.getDesiredTimeScale() )
                                                           .setHasMultipleSourcesPerSeries( false )
                                                           .setUnitMapper( super.getMeasurementUnitMapper() )
                                                           .setLeftOrRightOrBaseline( super.getLeftOrRightOrBaseline() )
                                                           .setTimeWindow( analysisRanges )
                                                           .setVariableFeatureId( super.getVariableFeatureId() )
                                                           .setReferenceTimeType( ReferenceTimeType.ANALYSIS_START_TIME )
                                                           //.setSeasonEnd(  )
                                                           //.setSeasonStart(  )
                                                           .build();
    }

    @Override
    boolean isForecast()
    {
        return false;
    }

    @Override
    public Optional<TimeSeries<Double>> get( long identifier )
    {
        throw new UnsupportedOperationException( "There is no existing identifier stored for an analysis timeseries, "
                                                 + "rather it is composed on demand." );
    }

    @Override
    public LongStream getAllIdentifiers()
    {
        throw new UnsupportedOperationException( "There are no identifiers stored for analysis timeseries." );
    }

    static class Builder extends TimeSeriesRetrieverBuilder<Double>
    {
        private Duration earliestAnalysisDuration = TimeWindow.DURATION_MIN;

        private Duration latestAnalysisDuration = TimeWindow.DURATION_MAX;

        private DuplicatePolicy duplicatePolicy = DuplicatePolicy.KEEP_ALL;

        /**
         * Sets the earliest analysis hour, if not <code>null</null>.
         * 
         * @param earliestAnalysisDuration duration
         * @return A builder
         */
        Builder setEarliestAnalysisDuration( Duration earliestAnalysisDuration )
        {
            if ( Objects.nonNull( earliestAnalysisDuration ) )
            {
                this.earliestAnalysisDuration = earliestAnalysisDuration;
            }

            return this;
        }

        /**
         * Set the latest analysis hour, if not <code>null</null>.
         * 
         * @param latestAnalysisDuration duration
         * @return A builder
         */
        Builder setLatestAnalysisDuration( Duration latestAnalysisDuration )
        {
            if ( Objects.nonNull( latestAnalysisDuration ) )
            {
                this.latestAnalysisDuration = latestAnalysisDuration;
            }

            return this;
        }

        /**
         * Set the duplicate policy, if not <code>null</null>.
         * 
         * @param latestAnalysisDuration duration
         * @return A builder
         */
        Builder setDuplicatePolicy( DuplicatePolicy duplicatePolicy )
        {
            if ( Objects.nonNull( duplicatePolicy ) )
            {
                this.duplicatePolicy = duplicatePolicy;
            }

            return this;
        }

        @Override
        TimeSeriesRetriever<Double> build()
        {
            return new AnalysisRetriever( this );
        }
    }

    /**
     * Get the analysis timeseries in one of several possible shapes and account for duplicates.
     */

    @Override
    public Stream<TimeSeries<Double>> get()
    {

        Stream<TimeSeries<Double>> timeSeries = this.individualAnalysisRetriever.get();

        // All events required?
        if ( !this.addOneTimeSeriesForEachAnalysisDuration() )
        {
            LOGGER.debug( "No discrete analysis times defined. Built a multi-event timeseries from the analysis." );

            // Apply a duplicate policy and return
            return this.applyDuplicatePolicy( timeSeries );
        }
        else
        {
            LOGGER.debug( "Building a single-event timeseries for each analysis duration between {} and {}.",
                          this.getEarliestAnalysisDuration(),
                          this.getLatestAnalysisDuration() );

            // No duplicate policy here, because we composed each time-series from common durations
            return this.createOneTimeSeriesForEachAnalysisDuration( timeSeries );
        }
    }

    /**
     * Transforms the input series to create one series for each required analysis duration.
     * 
     * @param timeSeries the input series to transform
     * @return one series for each analysis duration
     */

    private Stream<TimeSeries<Double>>
            createOneTimeSeriesForEachAnalysisDuration( Stream<TimeSeries<Double>> timeSeries )
    {

        // Filter the time-series. Create one new time-series for each event-by-duration within an existing 
        // time-series whose duration falls within the constraints
        List<TimeSeries<Double>> toStream = new ArrayList<>();

        List<TimeSeries<Double>> collection = timeSeries.collect( Collectors.toList() );
        for ( TimeSeries<Double> next : collection )
        {
            Map<Duration, Event<Double>> eventsByDuration =
                    TimeSeriesSlicer.mapEventsByDuration( next, ReferenceTimeType.ANALYSIS_START_TIME );

            for ( Entry<Duration, Event<Double>> nextSeries : eventsByDuration.entrySet() )
            {
                Duration duration = nextSeries.getKey();
                Event<Double> event = nextSeries.getValue();

                if ( duration.compareTo( this.getEarliestAnalysisDuration() ) >= 0
                     && duration.compareTo( this.getLatestAnalysisDuration() ) <= 0 )
                {
                    toStream.add( TimeSeries.of( next.getMetadata(),
                                                 new TreeSet<>( Collections.singleton( event ) ) ) );
                }
            }
        }

        // Warn if no events
        if ( toStream.isEmpty() )
        {
            LOGGER.warn( "While attempting to build a single-event timeseries for each analysis duration between {} "
                         + "and {}, failed to discover any events between those analysis durations.",
                         this.getEarliestAnalysisDuration(),
                         this.getLatestAnalysisDuration() );

        }
        else
        {
            LOGGER.debug( "Built {} single-event timeseries for each analysis duration between {} and {}: {}",
                          toStream.size(),
                          this.getEarliestAnalysisDuration(),
                          this.getLatestAnalysisDuration(),
                          toStream );
        }

        return toStream.stream();
    }

    /**
     * Returns the earliest analysis duration or null.
     * 
     * @return the earliest analysis duration or null
     */

    private Duration getEarliestAnalysisDuration()
    {
        return this.earliestAnalysisDuration;
    }

    /**
     * Returns the latest analysis duration or null.
     * 
     * @return the latest analysis duration or null
     */

    private Duration getLatestAnalysisDuration()
    {
        return this.latestAnalysisDuration;
    }

    /**
     * Returns <code>true</code> if the retriever should return one time-series for each of the common analysis 
     * durations across several reference times of the type {@link ANALYSIS_START_TIME}, <code>false</code> if it 
     * should return a single time-series per {@link ANALYSIS_START_TIME}.
     * 
     * @return true if the retriever should return a separate time-series for each analysis duration, otherwise false
     */

    private boolean addOneTimeSeriesForEachAnalysisDuration()
    {
        return !this.getEarliestAnalysisDuration().equals( TimeWindow.DURATION_MIN )
               || !this.getLatestAnalysisDuration().equals( TimeWindow.DURATION_MAX );
    }

    /**
     * Applies a duplicate policy to analysis time-series. One of {@link DuplicatePolicy@}.
     * 
     * @param timeSeries the input series whose duplicates, if any, should be treated
     * @return the time-series with duplicates treated
     */

    private Stream<TimeSeries<Double>> applyDuplicatePolicy( Stream<TimeSeries<Double>> timeSeries )
    {
        // Retain all
        if ( this.duplicatePolicy == DuplicatePolicy.KEEP_ALL )
        {
            return timeSeries;
        }

        // Filter, handling absence of reference times
        Comparator<Instant> nullsFriendly = Comparator.nullsFirst( Instant::compareTo );
        Comparator<TimeSeries<Double>> comparator =
                ( a, b ) -> nullsFriendly.compare( a.getReferenceTimes()
                                                    .get( ReferenceTimeType.ANALYSIS_START_TIME ),
                                                   b.getReferenceTimes()
                                                    .get( ReferenceTimeType.ANALYSIS_START_TIME ) );

        // Keep the duplicate with the earliest reference time
        if ( this.duplicatePolicy == DuplicatePolicy.KEEP_EARLIEST_REFERENCE_TIME )
        {
            return this.filterDuplicatesByValidTime( timeSeries, comparator );
        }
        // Keep the duplicate with the latest reference time
        else if ( this.duplicatePolicy == DuplicatePolicy.KEEP_LATEST_REFERENCE_TIME )
        {
            // Reversed: latest to earliest by ReferenceTimeType.ANALYSIS_START_TIME
            Comparator<TimeSeries<Double>> reversed = comparator.reversed();
            return this.filterDuplicatesByValidTime( timeSeries, reversed );
        }
        else
        {
            throw new IllegalStateException( "Encountered unexpected duplicate policy when filtering analysis "
                                             + "time-series for duiplicates: "
                                             + this.duplicatePolicy );
        }
    }

    /**
     * Filters the input time-series for duplicates using a prescribed comparator to order the time-series prior to
     * filtering. The first encountered duplicate, after ordering, will be retained. 
     * 
     * @param filterMe the time-series to filter
     * @param comparator the comparator for ordering the time-series
     * @return the filtered time-series with duplicates removed
     */

    private Stream<TimeSeries<Double>> filterDuplicatesByValidTime( Stream<TimeSeries<Double>> filterMe,
                                                                    Comparator<TimeSeries<Double>> comparator )
    {
        List<TimeSeries<Double>> collection = filterMe.collect( Collectors.toList() );

        // Sort the collection
        collection.sort( comparator );

        // Record the valid times consumed so far
        Set<Instant> validTimesConsumed = new HashSet<>();

        List<TimeSeries<Double>> toStream = new ArrayList<>();
        Comparator<Instant> nullsFriendly = Comparator.nullsFirst( Instant::compareTo );

        for ( TimeSeries<Double> next : collection )
        {
            TimeSeries<Double> filtered =
                    TimeSeriesSlicer.filterByEvent( next,
                                                    event -> !validTimesConsumed.contains( event.getTime() ) );

            // Add series with some events left and whose reference times fall within the reference time bounds
            // See: #74987-174. TODO: be explicit about the connection between reference times/types and declaration 
            // options. For now, reference times are not used to filter here
            Instant isGreaterThan = null;
            Instant isLessThanOrEqualTo = null;
            if ( Objects.nonNull( this.getTimeWindow() ) )
            {
                isGreaterThan = this.getTimeWindow().getEarliestReferenceTime();
                isLessThanOrEqualTo = this.getTimeWindow().getLatestReferenceTime();
            }

            Instant referenceTime = filtered.getReferenceTimes().get( ReferenceTimeType.ANALYSIS_START_TIME );

            // Some events left after filter and either no reference time bounds or reference times are within 
            // bounds
            if ( !filtered.getEvents().isEmpty() && Objects.isNull( this.getTimeWindow() )
                 || ( nullsFriendly.compare( referenceTime, isGreaterThan ) > 0
                      && nullsFriendly.compare( referenceTime, isLessThanOrEqualTo ) <= 0 ) )
            {
                toStream.add( filtered );

                // Get the valid times to ignore in subsequent series
                Set<Instant> nextValidTimes = next.getEvents()
                                                  .stream()
                                                  .map( Event::getTime )
                                                  .collect( Collectors.toSet() );

                validTimesConsumed.addAll( nextValidTimes );
            }

            if ( LOGGER.isTraceEnabled() && filtered.getEvents().size() != next.getEvents().size() )
            {
                LOGGER.trace( "While filtering analysis time-series {} according to the duplicate policy of {}, "
                              + "removed {} events that were duplicates by valid time across time-series.",
                              next.hashCode(),
                              this.duplicatePolicy,
                              next.getEvents().size() - filtered.getEvents().size() );
            }
        }

        return toStream.stream();
    }

}
