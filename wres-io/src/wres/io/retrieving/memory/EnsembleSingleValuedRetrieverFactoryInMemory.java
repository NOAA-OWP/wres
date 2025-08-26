package wres.io.retrieving.memory;

import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import wres.config.DeclarationUtilities;
import wres.config.components.DataType;
import wres.config.components.Dataset;
import wres.config.components.DatasetOrientation;
import wres.config.components.Variable;
import wres.datamodel.time.TimeWindowSlicer;
import wres.datamodel.types.Ensemble;
import wres.datamodel.space.Feature;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesStore;
import wres.datamodel.time.TimeWindowOuter;
import wres.io.project.Project;
import wres.io.retrieving.CachingRetriever;
import wres.io.retrieving.DuplicatePolicy;
import wres.io.retrieving.RetrieverFactory;
import wres.io.retrieving.RetrieverUtilities;
import wres.statistics.MessageUtilities;
import wres.statistics.generated.TimeWindow;

/**
 * <p>A factory class that creates retrievers for single-valued left datasets, ensemble right datasets and
 * single-valued baseline datasets associated with one evaluation. Backed by an in-memory {@link TimeSeriesStore}.
 *
 * @author James Brown
 */

public class EnsembleSingleValuedRetrieverFactoryInMemory implements RetrieverFactory<Double, Ensemble, Double>
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

    public static EnsembleSingleValuedRetrieverFactoryInMemory of( Project project,
                                                                   TimeSeriesStore timeSeriesStore )
    {
        return new EnsembleSingleValuedRetrieverFactoryInMemory( project, timeSeriesStore );
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
        Dataset data = DeclarationUtilities.getDeclaredDataset( this.project.getDeclaration(),
                                                                DatasetOrientation.LEFT );
        Variable variable = data.variable();

        Function<String, Stream<TimeSeries<Double>>> variableSupplier = name ->
                this.timeSeriesStore.getSingleValuedSeries( DatasetOrientation.LEFT,
                                                            features,
                                                            name );

        Stream<TimeSeries<Double>> timeSeries = this.getTimeSeries( variableSupplier,
                                                                    data,
                                                                    null,
                                                                    variable );

        Supplier<Stream<TimeSeries<Double>>> supplier =
                () -> timeSeries.map( s -> RetrieverUtilities.augmentTimeScale( s,
                                                                                DatasetOrientation.LEFT,
                                                                                data ) );
        // Wrap in a caching retriever
        return CachingRetriever.of( supplier );
    }

    @Override
    public Supplier<Stream<TimeSeries<Double>>> getLeftRetriever( Set<Feature> features,
                                                                  TimeWindowOuter timeWindow )
    {
        // Consider all possible lead durations
        TimeWindowOuter adjustedWindow =
                RetrieverUtilities.getTimeWindowWithUnconditionalLeadTimes( timeWindow,
                                                                            this.project.getDesiredTimeScale() );

        Dataset data = DeclarationUtilities.getDeclaredDataset( this.project.getDeclaration(),
                                                                DatasetOrientation.LEFT );
        adjustedWindow = RetrieverUtilities.adjustForAnalysisTypeIfRequired( adjustedWindow,
                                                                             data.type(),
                                                                             this.project.getEarliestAnalysisDuration(),
                                                                             this.project.getLatestAnalysisDuration() );

        Variable variable = data.variable();

        Function<String, Stream<TimeSeries<Double>>> variableSupplier = name ->
                this.timeSeriesStore.getSingleValuedSeries( DatasetOrientation.LEFT,
                                                            features,
                                                            name );

        Stream<TimeSeries<Double>> timeSeries = this.getTimeSeries( variableSupplier,
                                                                    data,
                                                                    adjustedWindow,
                                                                    variable );

        Supplier<Stream<TimeSeries<Double>>> supplier =
                () -> timeSeries.map( s -> RetrieverUtilities.augmentTimeScale( s,
                                                                                DatasetOrientation.LEFT,
                                                                                data ) );

        // Wrap in a caching retriever to allow re-use of left-ish data
        return CachingRetriever.of( supplier );
    }

    @Override
    public Supplier<Stream<TimeSeries<Ensemble>>> getRightRetriever( Set<Feature> features,
                                                                     TimeWindowOuter timeWindow )
    {
        TimeWindowOuter adjustedWindow = TimeWindowSlicer.adjustTimeWindowForTimeScale( timeWindow,
                                                                                        this.project.getDesiredTimeScale() );

        Dataset data = DeclarationUtilities.getDeclaredDataset( this.project.getDeclaration(),
                                                                DatasetOrientation.RIGHT );
        Variable variable = data.variable();

        TimeWindowOuter finalWindow =
                RetrieverUtilities.adjustForAnalysisTypeIfRequired( adjustedWindow,
                                                                    data.type(),
                                                                    this.project.getEarliestAnalysisDuration(),
                                                                    this.project.getLatestAnalysisDuration() );

        Function<String, Stream<TimeSeries<Ensemble>>> variableSupplier = name ->
                this.timeSeriesStore.getEnsembleSeries( finalWindow,
                                                        DatasetOrientation.RIGHT,
                                                        features,
                                                        name );

        Stream<TimeSeries<Ensemble>> timeSeries = this.getTimeSeries( variableSupplier,
                                                                      data,
                                                                      finalWindow,
                                                                      variable );

        return () -> timeSeries.map( s -> RetrieverUtilities.augmentTimeScale( s,
                                                                               DatasetOrientation.RIGHT,
                                                                               data ) );
    }

    @Override
    public Supplier<Stream<TimeSeries<Double>>> getBaselineRetriever( Set<Feature> features )
    {
        TimeWindow inner = MessageUtilities.getTimeWindow();
        TimeWindowOuter outer = TimeWindowOuter.of( inner );
        return this.getBaselineRetriever( features, outer );
    }

    @Override
    public Supplier<Stream<TimeSeries<Double>>> getBaselineRetriever( Set<Feature> features,
                                                                      TimeWindowOuter timeWindow )
    {
        TimeWindowOuter adjustedWindow = TimeWindowSlicer.adjustTimeWindowForTimeScale( timeWindow,
                                                                                        this.project.getDesiredTimeScale() );

        Dataset data = DeclarationUtilities.getDeclaredDataset( this.project.getDeclaration(),
                                                                DatasetOrientation.BASELINE );
        Variable variable = data.variable();

        TimeWindowOuter finalWindow =
                RetrieverUtilities.adjustForAnalysisTypeIfRequired( adjustedWindow,
                                                                    data.type(),
                                                                    this.project.getEarliestAnalysisDuration(),
                                                                    this.project.getLatestAnalysisDuration() );

        Function<String, Stream<TimeSeries<Double>>> variableSupplier = name ->
                this.timeSeriesStore.getSingleValuedSeries( finalWindow,
                                                            DatasetOrientation.BASELINE,
                                                            features,
                                                            name );

        Stream<TimeSeries<Double>> timeSeries = this.getTimeSeries( variableSupplier,
                                                                    data,
                                                                    finalWindow,
                                                                    variable );

        return () -> timeSeries.map( s -> RetrieverUtilities.augmentTimeScale( s,
                                                                               DatasetOrientation.BASELINE,
                                                                               data ) );
    }

    @Override
    public Supplier<Stream<TimeSeries<Double>>> getCovariateRetriever( Set<Feature> features, String variableName )
    {
        Objects.requireNonNull( features );
        Objects.requireNonNull( variableName );

        Dataset data = this.project.getCovariateDataset( variableName );
        Variable variable = data.variable();

        Function<String, Stream<TimeSeries<Double>>> variableSupplier = name ->
                this.timeSeriesStore.getSingleValuedSeries( DatasetOrientation.COVARIATE,
                                                            features,
                                                            name );

        Stream<TimeSeries<Double>> timeSeries = this.getTimeSeries( variableSupplier,
                                                                    data,
                                                                    null,
                                                                    variable );

        Supplier<Stream<TimeSeries<Double>>> supplier =
                () -> timeSeries.map( s -> RetrieverUtilities.augmentTimeScale( s,
                                                                                DatasetOrientation.COVARIATE,
                                                                                data ) );

        // Wrap in a caching retriever to allow re-use of left-ish data
        return CachingRetriever.of( supplier );
    }

    @Override
    public Supplier<Stream<TimeSeries<Double>>> getCovariateRetriever( Set<Feature> features,
                                                                       String variableName,
                                                                       TimeWindowOuter timeWindow )
    {
        // Consider all possible lead durations
        TimeWindowOuter adjustedWindow =
                RetrieverUtilities.getTimeWindowWithUnconditionalLeadTimes( timeWindow,
                                                                            this.project.getDesiredTimeScale() );

        Dataset data = this.project.getCovariateDataset( variableName );
        adjustedWindow = RetrieverUtilities.adjustForAnalysisTypeIfRequired( adjustedWindow,
                                                                             data.type(),
                                                                             this.project.getEarliestAnalysisDuration(),
                                                                             this.project.getLatestAnalysisDuration() );
        Variable variable = data.variable();

        Function<String, Stream<TimeSeries<Double>>> variableSupplier = name ->
                this.timeSeriesStore.getSingleValuedSeries( DatasetOrientation.COVARIATE,
                                                            features,
                                                            name );

        Stream<TimeSeries<Double>> timeSeries = this.getTimeSeries( variableSupplier,
                                                                    data,
                                                                    adjustedWindow,
                                                                    variable );

        Supplier<Stream<TimeSeries<Double>>> supplier =
                () -> timeSeries.map( s -> RetrieverUtilities.augmentTimeScale( s,
                                                                                DatasetOrientation.COVARIATE,
                                                                                data ) );

        // Wrap in a caching retriever
        return CachingRetriever.of( supplier );
    }

    /**
     * @param <T> the time-series data type
     * @param variableSupplier supplies a time-series by variable name
     * @param dataset the dataset
     * @param timeWindow the time window, optional
     * @param variable the variable
     * @return the time-series
     */

    private <T> Stream<TimeSeries<T>> getTimeSeries( Function<String, Stream<TimeSeries<T>>> variableSupplier,
                                                     Dataset dataset,
                                                     TimeWindowOuter timeWindow,
                                                     Variable variable )
    {
        Stream<TimeSeries<T>> allSeries = variableSupplier.apply( variable.name() );

        // Analysis shape of evaluation?
        if ( dataset.type() == DataType.ANALYSES )
        {
            allSeries = RetrieverUtilities.createAnalysisTimeSeries( allSeries,
                                                                     this.project.getEarliestAnalysisDuration(),
                                                                     this.project.getLatestAnalysisDuration(),
                                                                     DuplicatePolicy.KEEP_LATEST_REFERENCE_TIME,
                                                                     timeWindow );
        }

        // Add any time-series with aliased variable names
        for ( String alias : variable.aliases() )
        {
            Stream<TimeSeries<T>> innerSeries = variableSupplier.apply( alias );
            Stream<TimeSeries<T>> allSeriesFinal = allSeries;
            allSeries = Stream.concat( allSeriesFinal, innerSeries );
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

    private EnsembleSingleValuedRetrieverFactoryInMemory( Project project,
                                                          TimeSeriesStore timeSeriesStore )
    {
        Objects.requireNonNull( project );
        Objects.requireNonNull( timeSeriesStore );

        this.timeSeriesStore = timeSeriesStore;
        this.project = project;
    }

}
