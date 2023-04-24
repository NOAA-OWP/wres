package wres.io.retrieving.memory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.DatasourceType;
import wres.config.generated.LeftOrRightOrBaseline;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.space.Feature;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesSlicer;
import wres.datamodel.time.TimeSeriesStore;
import wres.datamodel.time.TimeWindowOuter;
import wres.io.project.Project;
import wres.io.retrieving.CachingRetriever;
import wres.io.retrieving.DuplicatePolicy;
import wres.io.retrieving.RetrieverFactory;
import wres.io.retrieving.RetrieverUtilities;
import wres.statistics.generated.TimeWindow;
import wres.statistics.generated.ReferenceTime.ReferenceTimeType;

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

    /** The project. */
    private final Project project;

    /**
     * Returns an instance.
     *
     * @param project the project
     * @param timeSeriesStore the store of time-series
     * @return a factory instance
     * @throws NullPointerException if any input is null
     */

    public static SingleValuedRetrieverFactoryInMemory of( Project project,
                                                           TimeSeriesStore timeSeriesStore )
    {
        return new SingleValuedRetrieverFactoryInMemory( project, timeSeriesStore );
    }

    @Override
    public Supplier<Stream<TimeSeries<Double>>> getClimatologyRetriever( Set<Feature> features )
    {
        // No distinction between climatology and left for now
        return this.getLeftRetriever( features );
    }

    @Override
    public Supplier<Stream<TimeSeries<Double>>> getLeftRetriever( Set<Feature> features )
    {
        Stream<TimeSeries<Double>> allSeries = this.getTimeSeries( LeftOrRightOrBaseline.LEFT, null, features );

        // Wrap in a caching retriever
        return CachingRetriever.of( () -> allSeries.map( timeSeries -> RetrieverUtilities.augmentTimeScale( timeSeries,
                                                                                                            LeftOrRightOrBaseline.LEFT,
                                                                                                            this.project.getDeclaredDataSource( LeftOrRightOrBaseline.LEFT ) ) ) );
    }

    @Override
    public Supplier<Stream<TimeSeries<Double>>> getLeftRetriever( Set<Feature> features,
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
                                                                                                            data ) ) );
    }

    @Override
    public Supplier<Stream<TimeSeries<Double>>> getRightRetriever( Set<Feature> features,
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
                                                                                       this.project.getDeclaredDataSource( LeftOrRightOrBaseline.RIGHT ) ) );
    }

    @Override
    public Supplier<Stream<TimeSeries<Double>>> getBaselineRetriever( Set<Feature> features )
    {
        TimeWindow inner = MessageFactory.getTimeWindow();
        TimeWindowOuter outer = TimeWindowOuter.of( inner );
        return this.getBaselineRetriever( features, outer );
    }

    @Override
    public Supplier<Stream<TimeSeries<Double>>> getBaselineRetriever( Set<Feature> features,
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
                                                                                       this.project.getDeclaredDataSource( LeftOrRightOrBaseline.BASELINE ) ) );
    }

    /**
     * @param orientation the orientation
     * @param timeWindow the time window, optional
     * @return the time-series
     */

    private Stream<TimeSeries<Double>> getTimeSeries( LeftOrRightOrBaseline orientation,
                                                      TimeWindowOuter timeWindow,
                                                      Set<Feature> features )
    {
        Stream<TimeSeries<Double>> allSeries;

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
        if ( this.project.usesGriddedData( orientation ) )
        {
            Map<Feature, List<TimeSeries<Double>>> outerGrouped =
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
     * Hidden constructor.
     * 
     * @param project the project
     * @param timeSeriesStore the store of time-series
     * @throws NullPointerException if any input is null
     */

    private SingleValuedRetrieverFactoryInMemory( Project project,
                                                  TimeSeriesStore timeSeriesStore )
    {
        Objects.requireNonNull( project );
        Objects.requireNonNull( timeSeriesStore );

        this.timeSeriesStore = timeSeriesStore;
        this.project = project;
    }

}
