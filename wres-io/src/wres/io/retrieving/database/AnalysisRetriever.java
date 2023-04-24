package wres.io.retrieving.database;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.DatasourceType;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeWindowOuter;
import wres.io.retrieving.DuplicatePolicy;
import wres.io.retrieving.RetrieverUtilities;
import wres.statistics.generated.ReferenceTime.ReferenceTimeType;

/**
 * <p>Retrieves data from the wres.TimeSeries and wres.TimeSeriesValue tables but
 * in the pattern expected for treating the nth timestep of each analysis as if
 * it were an event in a timeseries across analyses, sort of like observations.
 *
 * <p>The reason for separating it from forecast and observation timeseries
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
    private final DuplicatePolicy duplicatePolicy;

    private AnalysisRetriever( Builder builder )
    {
        super( builder, "metadata.reference_time", "TSV.lead" );
        this.earliestAnalysisDuration = builder.earliestAnalysisDuration;
        this.latestAnalysisDuration = builder.latestAnalysisDuration;
        this.duplicatePolicy = builder.duplicatePolicy;

        TimeWindowOuter analysisRanges = null;

        if ( Objects.nonNull( super.getTimeWindow() ) )
        {
            analysisRanges = RetrieverUtilities.adjustForAnalysisTypeIfRequired( super.getTimeWindow(),
                                                                                 DatasourceType.ANALYSES,
                                                                                 this.getEarliestAnalysisDuration(),
                                                                                 this.getLatestAnalysisDuration() );
        }

        LOGGER.debug( "Using a duplicate handling policy of {} for the retrieval of analysis time-series.",
                      this.duplicatePolicy );

        this.individualAnalysisRetriever =
                new SingleValuedForecastRetriever.Builder().setDatabase( super.getDatabase() )
                                                           .setFeaturesCache( super.getFeaturesCache() )
                                                           .setMeasurementUnitsCache( super.getMeasurementUnitsCache() )
                                                           .setProjectId( super.getProjectId() )
                                                           .setDeclaredExistingTimeScale( super.getDeclaredExistingTimeScale() )
                                                           .setDesiredTimeScale( super.getDesiredTimeScale() )
                                                           .setLeftOrRightOrBaseline( super.getLeftOrRightOrBaseline() )
                                                           .setTimeWindow( analysisRanges )
                                                           .setFeatures( super.getFeatures() )
                                                           .setVariableName( super.getVariableName() )
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

    static class Builder extends TimeSeriesRetriever.Builder<Double>
    {
        private Duration earliestAnalysisDuration = TimeWindowOuter.DURATION_MIN;

        private Duration latestAnalysisDuration = TimeWindowOuter.DURATION_MAX;

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

        return RetrieverUtilities.createAnalysisTimeSeries( timeSeries,
                                                            this.getEarliestAnalysisDuration(),
                                                            this.getLatestAnalysisDuration(),
                                                            this.duplicatePolicy,
                                                            super.getTimeWindow() );
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
}
