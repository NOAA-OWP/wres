package wres.io.retrieval;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.DatasourceType;
import wres.config.generated.LeftOrRightOrBaseline;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.space.FeatureKey;
import wres.datamodel.time.ReferenceTimeType;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesSlicer;
import wres.datamodel.time.TimeSeriesStore;
import wres.datamodel.time.TimeWindowOuter;
import wres.io.project.Project;
import wres.io.retrieval.AnalysisRetriever.DuplicatePolicy;
import wres.statistics.generated.TimeWindow;

/**
 * <p>A factory class that creates retrievers for the single-valued left and ensemble right datasets associated with one 
 * evaluation. Backed by an in-memory {@link TimeSeriesStore}.
 * 
 * @author James Brown
 */

public class SingleValuedRetrieverFactoryInMemory implements RetrieverFactory<Double, Double>
{
    /** A time-series store. */
    private final TimeSeriesStore timeSeriesStore;

    /**A unit mapper. */
    private final UnitMapper unitMapper;

    /** The project. */
    private final Project project;

    /**
     * Returns an instance.
     *
     * @param project the project
     * @param timeSeriesStore the store of time-series
     * @param unitMapper the unit mapper
     * @return a factory instance
     * @throws NullPointerException if any input is null
     */

    public static SingleValuedRetrieverFactoryInMemory of( Project project,
                                                           TimeSeriesStore timeSeriesStore,
                                                           UnitMapper unitMapper )
    {
        return new SingleValuedRetrieverFactoryInMemory( project, timeSeriesStore, unitMapper );
    }

    @Override
    public Supplier<Stream<TimeSeries<Double>>> getClimatologyRetriever( Set<FeatureKey> features )
    {
        // No distinction between climatology and left for now
        return this.getLeftRetriever( features );
    }

    @Override
    public Supplier<Stream<TimeSeries<Double>>> getLeftRetriever( Set<FeatureKey> features )
    {
        // Map the units
        UnaryOperator<TimeSeries<Double>> m = series -> RetrieverUtilities.mapUnits( series,
                                                                                     this.unitMapper.getUnitMapper( series.getMetadata()
                                                                                                                          .getUnit() )::applyAsDouble,
                                                                                     this.unitMapper.getDesiredMeasurementUnitName() );

        Stream<TimeSeries<Double>> allSeries = this.getTimeSeries( LeftOrRightOrBaseline.LEFT, null, features );

        // Wrap in a caching retriever
        return CachingRetriever.of( () -> allSeries.map( timeSeries -> RetrieverUtilities.augmentTimeScale( timeSeries,
                                                                                                            LeftOrRightOrBaseline.LEFT,
                                                                                                            this.project.getDeclaredDataSource( LeftOrRightOrBaseline.LEFT ) ) )
                                                   .map( m ) );
    }

    @Override
    public Supplier<Stream<TimeSeries<Double>>> getLeftRetriever( Set<FeatureKey> features,
                                                                  TimeWindowOuter timeWindow )
    {
        // Consider all possible lead durations
        com.google.protobuf.Duration lower =
                com.google.protobuf.Duration.newBuilder()
                                            .setSeconds( TimeWindowOuter.DURATION_MIN.getSeconds() )
                                            .setNanos( TimeWindowOuter.DURATION_MIN.getNano() )
                                            .build();

        com.google.protobuf.Duration upper =
                com.google.protobuf.Duration.newBuilder()
                                            .setSeconds( TimeWindowOuter.DURATION_MAX.getSeconds() )
                                            .setNanos( TimeWindowOuter.DURATION_MAX.getNano() )
                                            .build();

        TimeWindow inner = timeWindow.getTimeWindow()
                                     .toBuilder()
                                     .setEarliestLeadDuration( lower )
                                     .setLatestLeadDuration( upper )
                                     .build();

        TimeWindowOuter adjustedWindow = TimeSeriesSlicer.adjustByTimeScalePeriod( TimeWindowOuter.of( inner ),
                                                                                   this.project.getDesiredTimeScale() );

        DataSourceConfig data = this.project.getDeclaredDataSource( LeftOrRightOrBaseline.LEFT );
        adjustedWindow = RetrieverUtilities.adjustForAnalysisTypeIfRequired( adjustedWindow,
                                                                             data.getType(),
                                                                             this.project.getEarliestAnalysisDuration(),
                                                                             this.project.getLatestAnalysisDuration() );

        Stream<TimeSeries<Double>> allSeries = this.getTimeSeries( LeftOrRightOrBaseline.LEFT,
                                                                   adjustedWindow,
                                                                   features );

        // Wrap in a caching retriever to allow re-use of left-ish data
        return CachingRetriever.of( () -> allSeries.map( timeSeries -> RetrieverUtilities.augmentTimeScale( timeSeries,
                                                                                                            LeftOrRightOrBaseline.LEFT,
                                                                                                            data ) )
                                                   .map( this.getUnitMapper() ) );
    }

    @Override
    public Supplier<Stream<TimeSeries<Double>>> getRightRetriever( Set<FeatureKey> features,
                                                                   TimeWindowOuter timeWindow )
    {
        TimeWindowOuter adjustedWindow = TimeSeriesSlicer.adjustByTimeScalePeriod( timeWindow,
                                                                                   this.project.getDesiredTimeScale() );

        DataSourceConfig data = this.project.getDeclaredDataSource( LeftOrRightOrBaseline.RIGHT );
        adjustedWindow = RetrieverUtilities.adjustForAnalysisTypeIfRequired( adjustedWindow,
                                                                             data.getType(),
                                                                             this.project.getEarliestAnalysisDuration(),
                                                                             this.project.getLatestAnalysisDuration() );

        Stream<TimeSeries<Double>> allSeries = this.getTimeSeries( LeftOrRightOrBaseline.RIGHT,
                                                                   adjustedWindow,
                                                                   features );
        return () -> allSeries.map( timeSeries -> RetrieverUtilities.augmentTimeScale( timeSeries,
                                                                                       LeftOrRightOrBaseline.RIGHT,
                                                                                       this.project.getDeclaredDataSource( LeftOrRightOrBaseline.RIGHT ) ) )
                              .map( this.getUnitMapper() );
    }

    @Override
    public Supplier<Stream<TimeSeries<Double>>> getBaselineRetriever( Set<FeatureKey> features )
    {
        TimeWindow inner = MessageFactory.getTimeWindow();
        TimeWindowOuter outer = TimeWindowOuter.of( inner );
        return this.getBaselineRetriever( features, outer );
    }

    @Override
    public Supplier<Stream<TimeSeries<Double>>> getBaselineRetriever( Set<FeatureKey> features,
                                                                      TimeWindowOuter timeWindow )
    {
        TimeWindowOuter adjustedWindow = TimeSeriesSlicer.adjustByTimeScalePeriod( timeWindow,
                                                                                   this.project.getDesiredTimeScale() );

        DataSourceConfig data = this.project.getDeclaredDataSource( LeftOrRightOrBaseline.BASELINE );
        adjustedWindow = RetrieverUtilities.adjustForAnalysisTypeIfRequired( adjustedWindow,
                                                                             data.getType(),
                                                                             this.project.getEarliestAnalysisDuration(),
                                                                             this.project.getLatestAnalysisDuration() );

        Stream<TimeSeries<Double>> allSeries = this.getTimeSeries( LeftOrRightOrBaseline.BASELINE,
                                                                   adjustedWindow,
                                                                   features );
        return () -> allSeries.map( timeSeries -> RetrieverUtilities.augmentTimeScale( timeSeries,
                                                                                       LeftOrRightOrBaseline.BASELINE,
                                                                                       this.project.getDeclaredDataSource( LeftOrRightOrBaseline.BASELINE ) ) )
                              .map( this.getUnitMapper() );
    }

    /**
     * @param orientation the orientation
     * @param timeWindow the time window, optional
     * @return the time-series
     */

    private Stream<TimeSeries<Double>> getTimeSeries( LeftOrRightOrBaseline orientation,
                                                      TimeWindowOuter timeWindow,
                                                      Set<FeatureKey> features )
    {
        Stream<TimeSeries<Double>> allSeries = null;

        if ( Objects.isNull( timeWindow ) )
        {
            allSeries = this.timeSeriesStore.getSingleValuedSeries( orientation,
                                                                    features );
        }
        else
        {
            allSeries = this.timeSeriesStore.getSingleValuedSeries( timeWindow,
                                                                    orientation,
                                                                    features );
        }

        // Analysis shape of evaluation?
        if ( this.project.getDeclaredDataSource( orientation )
                         .getType() == DatasourceType.ANALYSES )
        {
            allSeries = RetrieverUtilities.createAnalysisTimeSeries( allSeries,
                                                                     this.project.getEarliestAnalysisDuration(),
                                                                     this.project.getLatestAnalysisDuration(),
                                                                     DuplicatePolicy.KEEP_LATEST_REFERENCE_TIME,
                                                                     timeWindow );
        }

        // Gridded data? If so, the time-series need to be consolidated because grids are read one-by-one into an
        // in-memory store. This results from gridded time-series data being read on "ingest" when using in-memory
        // mode versus "retrieval" when using a persistent store and "retrieval" mode allows multiple sources to be
        // collected together and read into a single time-series
        if ( this.project.usesGriddedData( this.project.getDeclaredDataSource( orientation ) ) )
        {
            Map<FeatureKey, List<TimeSeries<Double>>> outerGrouped =
                    allSeries.collect( Collectors.groupingBy( next -> next.getMetadata().getFeature() ) );

            // Iterate the series grouped by feature
            List<TimeSeries<Double>> outerGroup = new ArrayList<>();
            for ( List<TimeSeries<Double>> feature : outerGrouped.values() )
            {
                // Group the time-series by common reference times (including none) and then consolidate each group
                Map<Map<ReferenceTimeType, Instant>, List<TimeSeries<Double>>> innerGrouped =
                        feature.stream()
                               .collect( Collectors.groupingBy( TimeSeries::getReferenceTimes ) );

                for ( List<TimeSeries<Double>> nextGroup : innerGrouped.values() )
                {
                    TimeSeries<Double> next = TimeSeriesSlicer.consolidate( nextGroup );
                    outerGroup.add( next );
                }
            }

            allSeries = outerGroup.stream();
        }

        return allSeries;
    }

    /**
     * @return a unit mapper
     */

    private UnaryOperator<TimeSeries<Double>> getUnitMapper()
    {
        return series -> RetrieverUtilities.mapUnits( series,
                                                      this.unitMapper.getUnitMapper( series.getMetadata()
                                                                                           .getUnit() )::applyAsDouble,
                                                      this.unitMapper.getDesiredMeasurementUnitName() );
    }

    /**
     * Hidden constructor.
     * 
     * @param project the project
     * @param timeSeriesStore the time-series store
     * @param unitMapper the unit mapper
     * @param timeSeriesStore the store of time-series
     * @throws NullPointerException if any input is null
     */

    private SingleValuedRetrieverFactoryInMemory( Project project,
                                                  TimeSeriesStore timeSeriesStore,
                                                  UnitMapper unitMapper )
    {
        Objects.requireNonNull( project );
        Objects.requireNonNull( timeSeriesStore );
        Objects.requireNonNull( unitMapper );

        this.timeSeriesStore = timeSeriesStore;
        this.unitMapper = unitMapper;
        this.project = project;
    }

}
