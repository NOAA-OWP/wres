package wres.io.retrieval;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
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
    private static final Logger LOGGER = LoggerFactory.getLogger( AnalysisRetriever.class );

    private final TimeSeriesRetriever<Double> individualAnalysisRetriever;
    private final Duration earliestAnalysisDuration;
    private final Duration latestAnalysisDuration;

    private AnalysisRetriever( Builder builder )
    {
        super( builder, "TS.initialization_date", "TSV.lead" );
        this.earliestAnalysisDuration = builder.earliestAnalysisDuration;
        this.latestAnalysisDuration = builder.latestAnalysisDuration;

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

            analysisRanges = TimeWindow.of( Instant.MIN,
                                            Instant.MAX,
                                            originalRanges.getEarliestValidTime(),
                                            originalRanges.getLatestValidTime(),
                                            this.getEarliestAnalysisDuration(),
                                            this.getLatestAnalysisDuration() );
        }
        else
        {
            LOGGER.debug( "Building a multi-event analysis retriever." );

            analysisRanges = TimeWindow.of( Instant.MIN,
                                            Instant.MAX,
                                            originalRanges.getEarliestValidTime(),
                                            originalRanges.getLatestValidTime() );
        }

        this.individualAnalysisRetriever =
                new SingleValuedForecastRetriever.Builder()
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

        /**
         * Sets the earliest analysis hour, if not <code>null</null>.
         * 
         * @param earliestAnalysisDuration duration
         * @return A builder
         */
        Builder setEarliestAnalysisDuration( Duration earliestAnalysisDuration )
        {
            if( Objects.nonNull( earliestAnalysisDuration ) )
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
            if( Objects.nonNull( latestAnalysisDuration ) )
            {
                this.latestAnalysisDuration = latestAnalysisDuration;
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
     * Get the analysis timeseries, at the moment only one allowed, which is
     * composed of the nth event from each analysis within the overall bounds
     * of the project declaration.
     * @throws IllegalArgumentException When duplicate datetimes would be used.
     */

    @Override
    public Stream<TimeSeries<Double>> get()
    {

        Stream<TimeSeries<Double>> timeSeries = this.individualAnalysisRetriever.get();

        // All events required?
        if ( !this.addOneTimeSeriesForEachAnalysisDuration() )
        {
            LOGGER.debug( "No discrete analysis times defined. Built a multi-event timeseries from the analysis." );

            return timeSeries;
        }

        // Filter the time-series. Create one new time-series for each event-by-duration within an existing 
        // time-series whose duration falls within the constraints
        Set<TimeSeries<Double>> toStream = new HashSet<>();

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
                    toStream.add( TimeSeries.of( next.getReferenceTimes(),
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
    
}
