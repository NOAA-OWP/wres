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
    private final Duration analysisHour;

    private AnalysisRetriever( Builder builder )
    {
        super( builder, "TS.initialization_date", "TSV.lead" );
        this.analysisHour = builder.analysisHour;

        // Change the lead duration to the analysis step set by the user,
        // also set the reference datetime to an infinitely wide range so
        // that we do not restrict the analyses incorrectly.
        TimeWindow originalRanges = super.getTimeWindow();
        TimeWindow analysisRanges = originalRanges;

        if ( Objects.nonNull( this.analysisHour ) )
        {
            LOGGER.debug( "Building a single-event analysis retriever for analysis hour {}.", this.analysisHour );

            analysisRanges = TimeWindow.of( Instant.MIN,
                                            Instant.MAX,
                                            originalRanges.getEarliestValidTime(),
                                            originalRanges.getLatestValidTime(),
                                            this.analysisHour,
                                            this.analysisHour );
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
        throw new UnsupportedOperationException( "There is no existing identifier stored for an analysis timeseries, rather it is composed on demand." );
    }

    @Override
    public LongStream getAllIdentifiers()
    {
        throw new UnsupportedOperationException( "There are no identifiers stored for analysis timeseries." );
    }

    static class Builder extends TimeSeriesRetrieverBuilder<Double>
    {
        private Duration analysisHour = null;

        /**
         * Set the analysis hour, or leave at default when passing null
         * @param analysisHour duration or null
         * @return A builder
         */
        Builder setAnalysisHour( Duration analysisHour )
        {
            this.analysisHour = analysisHour;

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
        if ( Objects.isNull( this.analysisHour ) )
        {
            LOGGER.debug( "No discrete analysis times defined. Built a multi-event timeseries from the analysis." );

            return timeSeries;
        }
        
        // Filter the time-series. Create one new time-series for each event-by-duration within an existing 
        // time-series whose duration falls within the constraints
        Set<TimeSeries<Double>> toStream = new HashSet<>();
        
        List<TimeSeries<Double>> collection = timeSeries.collect( Collectors.toList() );
        for( TimeSeries<Double> next : collection )
        {
            Map<Duration, Event<Double>> eventsByDuration =
                    TimeSeriesSlicer.mapEventsByDuration( next, ReferenceTimeType.ANALYSIS_START_TIME );
            
            for( Entry<Duration,Event<Double>> nextSeries : eventsByDuration.entrySet() )
            {
                Duration duration = nextSeries.getKey();
                Event<Double> event = nextSeries.getValue();

                if ( duration.equals( this.analysisHour ) )
                {
                    toStream.add( TimeSeries.of( next.getReferenceTimes(),
                                                 new TreeSet<>( Collections.singleton( event ) ) ) );
                }
            }
        }
        
        // Warn if no events
        if ( toStream.isEmpty() )
        {
            LOGGER.warn( "While attempting to build single-event timeseries for each analysis time between {} and {}"
                         + ", failed to discover any time-series events between those analysis times.",
                         this.analysisHour,
                         this.analysisHour );

        }
        else
        {
            LOGGER.debug( "Built {} single-event timeseries for each analysis time between {} and {}: {}",
                          toStream.size(),
                          this.analysisHour,
                          this.analysisHour,
                          toStream );
        }

        return toStream.stream();
    }

}
