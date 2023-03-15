package wres.io.retrieval.memory;

import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.DatasourceType;
import wres.config.generated.LeftOrRightOrBaseline;
import wres.datamodel.Ensemble;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.space.Feature;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesSlicer;
import wres.datamodel.time.TimeSeriesStore;
import wres.datamodel.time.TimeWindowOuter;
import wres.io.project.Project;
import wres.io.retrieval.CachingRetriever;
import wres.io.retrieval.DuplicatePolicy;
import wres.io.retrieval.RetrieverFactory;
import wres.io.retrieval.RetrieverUtilities;
import wres.statistics.generated.TimeWindow;

/**
 * <p>A factory class that creates retrievers for the single-valued left and ensemble right datasets associated with one 
 * evaluation. Backed by an in-memory {@link TimeSeriesStore}.
 *
 * @author James Brown
 */

public class EnsembleRetrieverFactoryInMemory implements RetrieverFactory<Double, Ensemble>
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

    public static EnsembleRetrieverFactoryInMemory of( Project project,
                                                       TimeSeriesStore timeSeriesStore )
    {
        return new EnsembleRetrieverFactoryInMemory( project, timeSeriesStore );
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
        Stream<TimeSeries<Double>> originalSeries =
                this.timeSeriesStore.getSingleValuedSeries( LeftOrRightOrBaseline.LEFT,
                                                            features );

        Stream<TimeSeries<Double>> adaptedTimeSeries = this.getAdaptedTimeSeries( LeftOrRightOrBaseline.LEFT,
                                                                                  originalSeries,
                                                                                  null );
        // Wrap in a caching retriever
        return CachingRetriever.of( () -> adaptedTimeSeries.map( timeSeries ->
                                                                         RetrieverUtilities.augmentTimeScale( timeSeries,
                                                                                                              LeftOrRightOrBaseline.LEFT,
                                                                                                              this.project.getDeclaredDataSource(
                                                                                                                      LeftOrRightOrBaseline.LEFT ) ) ) );
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

        Stream<TimeSeries<Double>> originalSeries =
                this.timeSeriesStore.getSingleValuedSeries( adjustedWindow,
                                                            LeftOrRightOrBaseline.LEFT,
                                                            features );

        Stream<TimeSeries<Double>> adaptedTimeSeries = this.getAdaptedTimeSeries( LeftOrRightOrBaseline.LEFT,
                                                                                  originalSeries,
                                                                                  adjustedWindow );

        // Wrap in a caching retriever to allow re-use of left-ish data
        return CachingRetriever.of( () -> adaptedTimeSeries.map( timeSeries -> RetrieverUtilities.augmentTimeScale(
                timeSeries,
                LeftOrRightOrBaseline.LEFT,
                data ) ) );
    }

    @Override
    public Supplier<Stream<TimeSeries<Ensemble>>> getRightRetriever( Set<Feature> features,
                                                                     TimeWindowOuter timeWindow )
    {
        TimeWindowOuter adjustedWindow = TimeSeriesSlicer.adjustByTimeScalePeriod( timeWindow,
                                                                                   this.project.getDesiredTimeScale() );

        DataSourceConfig data = this.project.getDeclaredDataSource( LeftOrRightOrBaseline.RIGHT );
        adjustedWindow = RetrieverUtilities.adjustForAnalysisTypeIfRequired( adjustedWindow,
                                                                             data.getType(),
                                                                             this.project.getEarliestAnalysisDuration(),
                                                                             this.project.getLatestAnalysisDuration() );

        Stream<TimeSeries<Ensemble>> originalSeries =
                this.timeSeriesStore.getEnsembleSeries( adjustedWindow,
                                                        LeftOrRightOrBaseline.RIGHT,
                                                        features );

        Stream<TimeSeries<Ensemble>> adaptedTimeSeries = this.getAdaptedTimeSeries( LeftOrRightOrBaseline.RIGHT,
                                                                                    originalSeries,
                                                                                    adjustedWindow );

        return () -> adaptedTimeSeries.map( timeSeries -> RetrieverUtilities.augmentTimeScale( timeSeries,
                                                                                               LeftOrRightOrBaseline.RIGHT,
                                                                                               data ) );
    }

    @Override
    public Supplier<Stream<TimeSeries<Ensemble>>> getBaselineRetriever( Set<Feature> features )
    {
        TimeWindow inner = MessageFactory.getTimeWindow();
        TimeWindowOuter outer = TimeWindowOuter.of( inner );
        return this.getBaselineRetriever( features, outer );
    }

    @Override
    public Supplier<Stream<TimeSeries<Ensemble>>> getBaselineRetriever( Set<Feature> features,
                                                                        TimeWindowOuter timeWindow )
    {
        TimeWindowOuter adjustedWindow = TimeSeriesSlicer.adjustByTimeScalePeriod( timeWindow,
                                                                                   this.project.getDesiredTimeScale() );

        DataSourceConfig data = this.project.getDeclaredDataSource( LeftOrRightOrBaseline.BASELINE );
        adjustedWindow = RetrieverUtilities.adjustForAnalysisTypeIfRequired( adjustedWindow,
                                                                             data.getType(),
                                                                             this.project.getEarliestAnalysisDuration(),
                                                                             this.project.getLatestAnalysisDuration() );

        Stream<TimeSeries<Ensemble>> originalSeries =
                this.timeSeriesStore.getEnsembleSeries( adjustedWindow,
                                                        LeftOrRightOrBaseline.BASELINE,
                                                        features );

        Stream<TimeSeries<Ensemble>> adaptedTimeSeries = this.getAdaptedTimeSeries( LeftOrRightOrBaseline.BASELINE,
                                                                                    originalSeries,
                                                                                    adjustedWindow );

        return () -> adaptedTimeSeries.map( timeSeries -> RetrieverUtilities.augmentTimeScale( timeSeries,
                                                                                               LeftOrRightOrBaseline.BASELINE,
                                                                                               data ) );
    }


    /**
     * @param <T> the time-series event value type
     * @param orientation the orientation
     * @param timeSeries the input time-series
     * @param timeWindow the time window, optional
     * @return the adapted time-series
     */

    private <T> Stream<TimeSeries<T>> getAdaptedTimeSeries( LeftOrRightOrBaseline orientation,
                                                            Stream<TimeSeries<T>> timeSeries,
                                                            TimeWindowOuter timeWindow )
    {
        Stream<TimeSeries<T>> allSeries = timeSeries;

        // Analysis shape of evaluation?
        if ( this.project.getDeclaredDataSource( orientation )
                         .getType() == DatasourceType.ANALYSES )
        {
            allSeries = RetrieverUtilities.createAnalysisTimeSeries( timeSeries,
                                                                     this.project.getEarliestAnalysisDuration(),
                                                                     this.project.getLatestAnalysisDuration(),
                                                                     DuplicatePolicy.KEEP_LATEST_REFERENCE_TIME,
                                                                     timeWindow );
        }

        return allSeries;
    }

    /**
     * Hidden constructor.
     *
     * @param project the project
     * @param timeSeriesStore the time-series store
     * @throws NullPointerException if any input is null
     */

    private EnsembleRetrieverFactoryInMemory( Project project,
                                              TimeSeriesStore timeSeriesStore )
    {
        Objects.requireNonNull( project );
        Objects.requireNonNull( timeSeriesStore );

        this.timeSeriesStore = timeSeriesStore;
        this.project = project;
    }

}
