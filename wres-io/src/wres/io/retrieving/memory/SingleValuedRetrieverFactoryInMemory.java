package wres.io.retrieving.memory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import wres.config.yaml.DeclarationUtilities;
import wres.config.yaml.components.DataType;
import wres.config.yaml.components.Dataset;
import wres.config.yaml.components.DatasetOrientation;
import wres.config.yaml.components.Variable;
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
import wres.statistics.MessageFactory;
import wres.statistics.generated.TimeWindow;
import wres.statistics.generated.ReferenceTime.ReferenceTimeType;

/**
 * <p>A factory class that creates retrievers for the single-valued left and ensemble right datasets associated with one 
 * evaluation. Backed by an in-memory {@link TimeSeriesStore}.
 *
 * @author James Brown
 */

public class SingleValuedRetrieverFactoryInMemory implements RetrieverFactory<Double, Double, Double>
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
        // Wrap in a caching retriever
        Dataset data = DeclarationUtilities.getDeclaredDataset( this.project.getDeclaration(),
                                                                DatasetOrientation.LEFT );
        Variable variable = data.variable();

        Stream<TimeSeries<Double>> allSeries = this.getTimeSeries( DatasetOrientation.LEFT,
                                                                   data,
                                                                   features,
                                                                   null,
                                                                   variable );
        Supplier<Stream<TimeSeries<Double>>> supplier =
                () -> allSeries.map( timeSeries ->
                                             RetrieverUtilities.augmentTimeScale( timeSeries,
                                                                                  DatasetOrientation.LEFT,
                                                                                  data ) );
        return CachingRetriever.of( supplier );
    }

    @Override
    public Supplier<Stream<TimeSeries<Double>>> getLeftRetriever( Set<Feature> features,
                                                                  TimeWindowOuter timeWindow )
    {
        TimeWindowOuter adjustedWindow =
                RetrieverUtilities.getTimeWindowWithUnconditionalLeadTimes( timeWindow,
                                                                            this.project.getDesiredTimeScale() );

        Dataset data = DeclarationUtilities.getDeclaredDataset( this.project.getDeclaration(),
                                                                DatasetOrientation.LEFT );
        Variable variable = data.variable();

        adjustedWindow = RetrieverUtilities.adjustForAnalysisTypeIfRequired( adjustedWindow,
                                                                             data.type(),
                                                                             this.project.getEarliestAnalysisDuration(),
                                                                             this.project.getLatestAnalysisDuration() );

        Stream<TimeSeries<Double>> allSeries = this.getTimeSeries( DatasetOrientation.LEFT,
                                                                   data,
                                                                   features,
                                                                   adjustedWindow,
                                                                   variable );

        // Wrap in a caching retriever to allow re-use of left-ish data
        return CachingRetriever.of( () -> allSeries.map( timeSeries -> RetrieverUtilities.augmentTimeScale( timeSeries,
                                                                                                            DatasetOrientation.LEFT,
                                                                                                            data ) ) );
    }

    @Override
    public Supplier<Stream<TimeSeries<Double>>> getRightRetriever( Set<Feature> features,
                                                                   TimeWindowOuter timeWindow )
    {
        TimeWindowOuter adjustedWindow = TimeSeriesSlicer.adjustTimeWindowForTimeScale( timeWindow,
                                                                                        this.project.getDesiredTimeScale() );

        Dataset data = DeclarationUtilities.getDeclaredDataset( this.project.getDeclaration(),
                                                                DatasetOrientation.RIGHT );
        Variable variable = data.variable();

        adjustedWindow = RetrieverUtilities.adjustForAnalysisTypeIfRequired( adjustedWindow,
                                                                             data.type(),
                                                                             this.project.getEarliestAnalysisDuration(),
                                                                             this.project.getLatestAnalysisDuration() );

        Stream<TimeSeries<Double>> allSeries = this.getTimeSeries( DatasetOrientation.RIGHT,
                                                                   data,
                                                                   features,
                                                                   adjustedWindow,
                                                                   variable );

        Supplier<Stream<TimeSeries<Double>>> supplier =
                () -> allSeries.map( timeSeries ->
                                             RetrieverUtilities.augmentTimeScale( timeSeries,
                                                                                  DatasetOrientation.RIGHT,
                                                                                  data ) );
        return CachingRetriever.of( supplier );
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
        TimeWindowOuter adjustedWindow = TimeSeriesSlicer.adjustTimeWindowForTimeScale( timeWindow,
                                                                                        this.project.getDesiredTimeScale() );

        Dataset data = DeclarationUtilities.getDeclaredDataset( this.project.getDeclaration(),
                                                                DatasetOrientation.BASELINE );
        Variable variable = data.variable();

        adjustedWindow = RetrieverUtilities.adjustForAnalysisTypeIfRequired( adjustedWindow,
                                                                             data.type(),
                                                                             this.project.getEarliestAnalysisDuration(),
                                                                             this.project.getLatestAnalysisDuration() );

        Stream<TimeSeries<Double>> allSeries = this.getTimeSeries( DatasetOrientation.BASELINE,
                                                                   data,
                                                                   features,
                                                                   adjustedWindow,
                                                                   variable );
        Supplier<Stream<TimeSeries<Double>>> supplier =
                () -> allSeries.map( timeSeries ->
                                             RetrieverUtilities.augmentTimeScale( timeSeries,
                                                                                  DatasetOrientation.BASELINE,
                                                                                  data ) );
        return CachingRetriever.of( supplier );
    }


    @Override
    public Supplier<Stream<TimeSeries<Double>>> getCovariateRetriever( Set<Feature> features, String variableName )
    {
        // Wrap in a caching retriever
        Dataset data = this.project.getCovariateDataset( variableName );
        Variable variable = data.variable();

        Stream<TimeSeries<Double>> allSeries = this.getTimeSeries( DatasetOrientation.COVARIATE,
                                                                   data,
                                                                   features,
                                                                   null,
                                                                   variable );

        Supplier<Stream<TimeSeries<Double>>> supplier =
                () -> allSeries.map( timeSeries ->
                                             RetrieverUtilities.augmentTimeScale( timeSeries,
                                                                                  DatasetOrientation.COVARIATE,
                                                                                  data ) );
        return CachingRetriever.of( supplier );
    }

    @Override
    public Supplier<Stream<TimeSeries<Double>>> getCovariateRetriever( Set<Feature> features,
                                                                       String variableName,
                                                                       TimeWindowOuter timeWindow )
    {
        TimeWindowOuter adjustedWindow =
                RetrieverUtilities.getTimeWindowWithUnconditionalLeadTimes( timeWindow,
                                                                            this.project.getDesiredTimeScale() );

        Dataset data = this.project.getCovariateDataset( variableName );
        Variable variable = data.variable();

        adjustedWindow = RetrieverUtilities.adjustForAnalysisTypeIfRequired( adjustedWindow,
                                                                             data.type(),
                                                                             this.project.getEarliestAnalysisDuration(),
                                                                             this.project.getLatestAnalysisDuration() );

        Stream<TimeSeries<Double>> allSeries = this.getTimeSeries( DatasetOrientation.COVARIATE,
                                                                   data,
                                                                   features,
                                                                   adjustedWindow,
                                                                   variable );

        // Wrap in a caching retriever to allow re-use of left-ish data
        return CachingRetriever.of( () -> allSeries.map( timeSeries -> RetrieverUtilities.augmentTimeScale( timeSeries,
                                                                                                            DatasetOrientation.LEFT,
                                                                                                            data ) ) );

    }

    /**
     * @param orientation the orientation
     * @param dataset the dataset
     * @param features the features
     * @param timeWindow the time window, optional
     * @param variable the variable
     * @return the time-series
     */

    private Stream<TimeSeries<Double>> getTimeSeries( DatasetOrientation orientation,
                                                      Dataset dataset,
                                                      Set<Feature> features,
                                                      TimeWindowOuter timeWindow,
                                                      Variable variable )
    {
        Stream<TimeSeries<Double>> allSeries = Stream.of();

        Set<String> names = new HashSet<>( variable.aliases() );

        // Add the main variable name
        names.add( variable.name() );

        // Add any time-series with aliased variable names
        for ( String alias : names )
        {
            Stream<TimeSeries<Double>> innerSeries;
            if ( Objects.isNull( timeWindow ) )
            {
                innerSeries = this.timeSeriesStore.getSingleValuedSeries( orientation,
                                                                          features,
                                                                          alias );
            }
            else
            {
                innerSeries = this.timeSeriesStore.getSingleValuedSeries( timeWindow,
                                                                          orientation,
                                                                          features,
                                                                          alias );
            }
            Stream<TimeSeries<Double>> allSeriesFinal = allSeries;
            allSeries = Stream.concat( allSeriesFinal, innerSeries );
        }

        // Analysis shape of evaluation?
        if ( dataset.type() == DataType.ANALYSES )
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
                    allSeries.collect( Collectors.groupingBy( next -> next.getMetadata()
                                                                          .getFeature() ) );

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
