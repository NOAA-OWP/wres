package wres.io.retrieval.datashop;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.time.Event;
import wres.datamodel.time.ReferenceTimeType;
import wres.datamodel.time.TimeSeries;
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

public class AnalysisRetriever extends TimeSeriesRetriever<Double>
{
    private static final Logger LOGGER = LoggerFactory.getLogger( AnalysisRetriever.class );

    private final TimeSeriesRetriever<Double> individualAnalysisRetriever;
    private final Duration analysisMember;

    private AnalysisRetriever( TimeSeriesRetrieverBuilder<Double> builder )
    {
        super( builder, "TS.initialization_date", "TSV.lead" );

        // TODO: user-set, not zero.
        this.analysisMember = Duration.ZERO;
        // Change the lead duration to the analysis step set by the user,
        // also set the reference datetime to an infinitely wide range so
        // that we do not restrict the analyses incorrectly.
        TimeWindow originalRanges = super.getTimeWindow();
        TimeWindow analysisRanges = TimeWindow.of( Instant.MIN,
                                                   Instant.MAX,
                                                   originalRanges.getEarliestValidTime(),
                                                   originalRanges.getLatestValidTime(),
                                                   this.analysisMember,
                                                   this.analysisMember );
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
        return null;
    }

    static class Builder extends TimeSeriesRetrieverBuilder<Double>
    {
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
     */

    @Override
    public Stream<TimeSeries<Double>> get()
    {
        TimeSeries.TimeSeriesBuilder<Double> timeSeriesBuilder =
                new TimeSeries.TimeSeriesBuilder<>();
        SingleEventExtractor eventExtractor = new SingleEventExtractor( this.analysisMember );
        this.individualAnalysisRetriever.get()
                                        .map( eventExtractor )
                                        .filter( Objects::nonNull )
                                        .forEach( timeSeriesBuilder::addEvent );
        TimeSeries<Double> timeSeries = timeSeriesBuilder.build();
        LOGGER.debug( "Built an analysis timeseries: {}", timeSeries );
        return Stream.of( timeSeries );
    }


    /**
     * Extracts a single event where the difference between the reference
     * datetime and the valid datetime matches the given (on construction)
     * Duration.
     */

    private static final class SingleEventExtractor implements Function<TimeSeries<Double>,Event<Double>>
    {
        private final Duration analysisMember;

        SingleEventExtractor( Duration analysisMember )
        {
            this.analysisMember = analysisMember;
        }

        /**
         * Extracts a single event where the difference between the reference
         * datetime and the valid datetime matches the given (on construction)
         * Duration.
         * @return A single matching event or null when not found.
         * @throws UnsupportedOperationException If no reference datetime found.
         */

        @Override
        public Event<Double> apply( TimeSeries<Double> timeSeries )
        {
            Instant analysisReference = timeSeries.getReferenceTimes()
                                                  .get( ReferenceTimeType.T0 );

            if ( Objects.isNull( analysisReference) )
            {
                analysisReference = timeSeries.getReferenceTimes()
                                              .get( ReferenceTimeType.UNKNOWN );
            }

            if ( Objects.isNull( analysisReference )
                 || analysisReference.equals( Instant.MIN )
                 || analysisReference.equals( Instant.MAX ) )
            {
                throw new UnsupportedOperationException( "Unable to find a valid reference datetime for analysis dataset "
                                                         + timeSeries );
            }

            for ( Event<Double> event : timeSeries.getEvents() )
            {
                Duration durationFromReference = Duration.between( analysisReference,
                                                                   event.getTime() );
                if ( durationFromReference.equals( this.analysisMember ) )
                {
                    return event;
                }
            }

            LOGGER.warn( "No analysis member matching {} found in timeseries, skipping {}",
                         this.analysisMember, timeSeries );
            return null;
        }
    }
}
