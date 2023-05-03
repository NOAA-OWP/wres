package wres.io.retrieving.database;

import java.time.Duration;
import java.time.MonthDay;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.yaml.DeclarationUtilities;
import wres.config.yaml.components.DataType;
import wres.config.yaml.components.Dataset;
import wres.config.yaml.components.DatasetOrientation;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.Feature;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeWindowOuter;
import wres.io.database.caching.DatabaseCaches;
import wres.io.database.caching.Features;
import wres.io.database.caching.MeasurementUnits;
import wres.io.database.Database;
import wres.io.project.Project;
import wres.io.retrieving.DataAccessException;
import wres.io.retrieving.RetrieverFactory;
import wres.io.retrieving.DuplicatePolicy;
import wres.statistics.generated.ReferenceTime.ReferenceTimeType;

/**
 * <p>A factory class that creates retrievers for the single-valued left and right datasets associated with one 
 * evaluation. This factory takes a "per-feature-view" of retrieval whereby a feature is supplied on construction.
 * Other possible implementations include a multiple-feature-view or a grid-view.
 *
 * @author James Brown
 */

public class SingleValuedRetrieverFactory implements RetrieverFactory<Double, Double>
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( SingleValuedRetrieverFactory.class );

    /** Message about features, re-used several times. */
    private static final String FEATURE_MESSAGE = ", features ";

    /** Message about time windows, re-used several times. */
    private static final String AND_TIME_WINDOW_MESSAGE = " and time window ";

    /** The project. */
    private final Project project;

    /** The database. */
    private final Database database;

    /** The caches/ORMs. */
    private final DatabaseCaches caches;

    /** Left data declaration. */
    private final Dataset leftDataset;

    /** Right data declaration. */
    private final Dataset rightDataset;

    /** Baseline data declaration. */
    private final Dataset baselineDataset;

    /** Start of a seasonal constraint, if any. */
    private final MonthDay seasonStart;

    /** End of a seasonal constraint, if any. */
    private final MonthDay seasonEnd;

    /** Declared <code>desiredTimeScale</code>, if any. */
    private final TimeScaleOuter desiredTimeScale;

    /**
     * Returns an instance.
     *
     * @param project the project
     * @param database the database
     * @param caches the caches
     * @return a factory instance
     * @throws NullPointerException if any input is null
     */

    public static SingleValuedRetrieverFactory of( Project project,
                                                   Database database,
                                                   DatabaseCaches caches )
    {
        return new SingleValuedRetrieverFactory( project,
                                                 database,
                                                 caches );
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
        return this.get( this.leftDataset,
                         DatasetOrientation.LEFT,
                         features,
                         null );
    }

    @Override
    public Supplier<Stream<TimeSeries<Double>>> getLeftRetriever( Set<Feature> features,
                                                                  TimeWindowOuter timeWindow )
    {
        return this.get( this.leftDataset,
                         DatasetOrientation.LEFT,
                         features,
                         timeWindow );
    }

    @Override
    public Supplier<Stream<TimeSeries<Double>>> getRightRetriever( Set<Feature> features,
                                                                   TimeWindowOuter timeWindow )
    {
        return this.get( this.rightDataset,
                         DatasetOrientation.RIGHT,
                         features,
                         timeWindow );
    }

    @Override
    public Supplier<Stream<TimeSeries<Double>>> getBaselineRetriever( Set<Feature> features )
    {
        return this.get( this.baselineDataset,
                         DatasetOrientation.BASELINE,
                         features,
                         null );
    }

    @Override
    public Supplier<Stream<TimeSeries<Double>>> getBaselineRetriever( Set<Feature> features,
                                                                      TimeWindowOuter timeWindow )
    {
        return this.get( this.baselineDataset,
                         DatasetOrientation.BASELINE,
                         features,
                         timeWindow );
    }

    /**
     * Returns a supplier of time-series.
     *
     * @param dataset the data source configuration
     * @param orientation the orientation of the data source
     * @param features the features
     * @param timeWindow the time window
     * @return the supplier
     */

    private Supplier<Stream<TimeSeries<Double>>> get( Dataset dataset,
                                                      DatasetOrientation orientation,
                                                      Set<Feature> features,
                                                      TimeWindowOuter timeWindow )
    {
        Objects.requireNonNull( dataset );
        Objects.requireNonNull( orientation );
        Objects.requireNonNull( features );

        LOGGER.debug( "Creating a {} retriever for project '{}', features '{}' and time window {}.",
                      orientation,
                      this.getProject().getId(),
                      features,
                      timeWindow );
        TimeSeriesRetriever.Builder<Double> builder;

        boolean isConfiguredAsForecast = DeclarationUtilities.isForecast( dataset );
        String variableName = this.getProject()
                                  .getVariableName( orientation );
        TimeScaleOuter declaredExistingTimeScale =
                this.getDeclaredExistingTimeScale( dataset );

        try
        {
            // Gridded data?
            if ( this.getProject().usesGriddedData( orientation ) )
            {
                builder = this.getGriddedRetrieverBuilder( dataset.type() )
                              .setIsForecast( isConfiguredAsForecast )
                              .setFeatures( features );
            }
            else
            {
                builder = this.getRetrieverBuilder( dataset.type() )
                              .setFeatures( features );
            }
        }
        catch ( DataAccessException e )
        {
            throw new DataAccessException( "While creating a retriever of "
                                           + orientation
                                           + " data for project "
                                           + this.getProject()
                                                 .getId()
                                           + FEATURE_MESSAGE
                                           + features
                                           + AND_TIME_WINDOW_MESSAGE
                                           + timeWindow
                                           + ":",
                                           e );
        }

        builder.setDatabase( this.getDatabase() )
               .setFeaturesCache( this.getFeaturesCache() )
               .setMeasurementUnitsCache( this.getMeasurementUnitsCache() )
               .setProjectId( this.getProject()
                                  .getId() )
               .setVariableName( variableName )
               .setDatasetOrientation( orientation )
               .setDeclaredExistingTimeScale( declaredExistingTimeScale )
               .setDesiredTimeScale( this.desiredTimeScale );

        if ( Objects.nonNull( timeWindow ) )
        {
            builder.setTimeWindow( timeWindow );
        }

        // TODO: reconsider how seasons are applied. For now, do not apply to
        // left-ish data because the current interpretation of right-ish data
        // with a forecast type is to use reference time, not valid time. See #40405

        if ( orientation != DatasetOrientation.LEFT )
        {
            builder.setSeasonStart( this.seasonStart )
                   .setSeasonEnd( this.seasonEnd );
        }

        return builder.build();
    }

    /**
     * @return the database.
     */

    private Database getDatabase()
    {
        return this.database;
    }

    /**
     * @return the features cache.
     */

    private Features getFeaturesCache()
    {
        return this.caches.getFeaturesCache();
    }

    /**
     * @return the measurement units cache.
     */

    private MeasurementUnits getMeasurementUnitsCache()
    {
        return this.caches.getMeasurementUnitsCache();
    }

    /**
     * Returns a builder for a retriever.
     *
     * @param dataType the retrieved data type
     * @return the retriever
     * @throws IllegalArgumentException if the data type is unrecognized in this context
     */

    private TimeSeriesRetriever.Builder<Double> getRetrieverBuilder( DataType dataType )
    {
        Duration earliestAnalysisDuration = this.getProject()
                                                .getEarliestAnalysisDuration();
        Duration latestAnalysisDuration = this.getProject()
                                              .getLatestAnalysisDuration();

        return switch ( dataType )
                {
                    case SINGLE_VALUED_FORECASTS ->
                            new SingleValuedForecastRetriever.Builder().setReferenceTimeType( ReferenceTimeType.T0 );
                    case OBSERVATIONS -> new ObservationRetriever.Builder();
                    case SIMULATIONS ->
                            new ObservationRetriever.Builder().setReferenceTimeType( ReferenceTimeType.ANALYSIS_START_TIME );
                    case ANALYSES ->
                            new AnalysisRetriever.Builder().setEarliestAnalysisDuration( earliestAnalysisDuration )
                                                           .setLatestAnalysisDuration( latestAnalysisDuration )
                                                           .setDuplicatePolicy( DuplicatePolicy.KEEP_LATEST_REFERENCE_TIME )
                                                           .setReferenceTimeType( ReferenceTimeType.ANALYSIS_START_TIME );
                    default -> throw new IllegalArgumentException(
                            "Unrecognized data type from which to create the single-valued "
                            + "retriever: "
                            + dataType
                            + "'." );
                };
    }

    /**
     * Returns a builder for a gridded retriever.
     *
     * @param dataType the retrieved data type
     * @return the retriever
     * @throws IllegalArgumentException if the data type is unrecognized in this context
     */

    private SingleValuedGriddedRetriever.Builder getGriddedRetrieverBuilder( DataType dataType )
    {
        return switch ( dataType )
                {
                    case SINGLE_VALUED_FORECASTS ->
                            ( SingleValuedGriddedRetriever.Builder ) new SingleValuedGriddedRetriever.Builder()
                                    .setIsForecast( true )
                                    .setReferenceTimeType(
                                            ReferenceTimeType.T0 );
                    case OBSERVATIONS, SIMULATIONS ->
                            ( SingleValuedGriddedRetriever.Builder ) new SingleValuedGriddedRetriever.Builder()
                                    .setReferenceTimeType( ReferenceTimeType.ANALYSIS_START_TIME );
                    default -> throw new IllegalArgumentException(
                            "Unrecognized data type from which to create the single-valued "
                            + "retriever: "
                            + dataType
                            + "'." );
                };
    }

    /**
     * Returns the declared existing timescale associated with a dataset, if any.
     *
     * @param dataset the dataset declaration
     * @return a declared existing time scale, or null
     */

    private TimeScaleOuter getDeclaredExistingTimeScale( Dataset dataset )
    {
        // Declared existing scale, which can be used to augment a source
        wres.config.yaml.components.TimeScale declaredExistingTimeScaleInner = dataset.timeScale();
        TimeScaleOuter declaredExistingTimeScale = null;

        if ( Objects.nonNull( declaredExistingTimeScaleInner ) )
        {
            declaredExistingTimeScale = TimeScaleOuter.of( declaredExistingTimeScaleInner.timeScale() );
        }

        return declaredExistingTimeScale;
    }

    /**
     * Returns the project associated with this factory instance.
     *
     * @return the project
     */

    private Project getProject()
    {
        return this.project;
    }

    /**
     * Hidden constructor.
     *
     * @param project the project
     * @param database the database,
     * @param caches the caches
     * @throws NullPointerException if any input is null
     */

    private SingleValuedRetrieverFactory( Project project,
                                          Database database,
                                          DatabaseCaches caches )
    {
        Objects.requireNonNull( project );
        Objects.requireNonNull( database );
        Objects.requireNonNull( caches );

        this.project = project;
        this.database = database;
        this.caches = caches;

        this.leftDataset = project.getDeclaredDataset( DatasetOrientation.LEFT );
        this.rightDataset = project.getDeclaredDataset( DatasetOrientation.RIGHT );
        this.baselineDataset = project.getDeclaredDataset( DatasetOrientation.BASELINE );

        // Obtain any seasonal constraints
        this.seasonStart = project.getStartOfSeason();
        this.seasonEnd = project.getEndOfSeason();

        // Obtain and set the desired timescale.
        this.desiredTimeScale = project.getDesiredTimeScale();
    }

}
