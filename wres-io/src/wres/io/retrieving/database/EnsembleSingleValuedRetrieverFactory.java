package wres.io.retrieving.database;

import java.time.Duration;
import java.time.MonthDay;
import java.util.Arrays;
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
import wres.datamodel.types.Ensemble;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.Feature;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeWindowOuter;
import wres.io.database.Database;
import wres.io.database.caching.DatabaseCaches;
import wres.io.database.caching.Ensembles;
import wres.io.database.caching.Features;
import wres.io.database.caching.MeasurementUnits;
import wres.io.project.Project;
import wres.io.retrieving.DuplicatePolicy;
import wres.io.retrieving.RetrieverFactory;

/**
 * <p>A factory class that creates retrievers for single-valued left datasets, ensemble right datasets and
 * single-valued baseline datasets associated with one evaluation.
 *
 * @author James Brown
 */

public class EnsembleSingleValuedRetrieverFactory implements RetrieverFactory<Double, Ensemble, Double>
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( EnsembleSingleValuedRetrieverFactory.class );

    /** The project. */
    private final Project project;

    /** The database. */
    private final Database database;

    /** The caches/ORMs. */
    private final DatabaseCaches caches;

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

    /** A single-valued retriever factory for the left-ish data. */
    private final RetrieverFactory<Double, Double, Double> leftFactory;

    /**
     * Returns an instance.
     *
     * @param project the project
     * @param database the database
     * @param caches the caches
     * @return a factory instance
     * @throws NullPointerException if any input is null
     */

    public static EnsembleSingleValuedRetrieverFactory of( Project project,
                                                           Database database,
                                                           DatabaseCaches caches )
    {
        return new EnsembleSingleValuedRetrieverFactory( project, database, caches );
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
        return this.leftFactory.getLeftRetriever( features );
    }

    @Override
    public Supplier<Stream<TimeSeries<Double>>> getLeftRetriever( Set<Feature> features,
                                                                  TimeWindowOuter timeWindow )
    {
        return this.leftFactory.getLeftRetriever( features, timeWindow );
    }

    @Override
    public Supplier<Stream<TimeSeries<Ensemble>>> getRightRetriever( Set<Feature> features,
                                                                     TimeWindowOuter timeWindow )
    {
        LOGGER.debug( "Creating a right retriever for project '{}', features '{}' and time window {}.",
                      this.project.getId(),
                      features,
                      timeWindow );

        return new EnsembleForecastRetriever.Builder().setEnsemblesCache( this.getEnsemblesCache() )
                                                      .setDatabase( this.getDatabase() )
                                                      .setFeaturesCache( this.getFeaturesCache() )
                                                      .setMeasurementUnitsCache( this.getMeasurementUnitsCache() )
                                                      .setProjectId( this.project.getId() )
                                                      .setFeatures( features )
                                                      .setVariableName( this.project.getRightVariable()
                                                                                .name() )
                                                      .setDatasetOrientation( DatasetOrientation.RIGHT )
                                                      .setDeclaredExistingTimeScale( this.getDeclaredExistingTimeScale(
                                                              rightDataset ) )
                                                      .setDesiredTimeScale( this.desiredTimeScale )
                                                      .setSeasonStart( this.seasonStart )
                                                      .setSeasonEnd( this.seasonEnd )
                                                      .setTimeWindow( timeWindow )
                                                      .build();
    }

    @Override
    public Supplier<Stream<TimeSeries<Double>>> getBaselineRetriever( Set<Feature> features )
    {
        return this.getBaselineRetriever( features, null );
    }

    @Override
    public Supplier<Stream<TimeSeries<Double>>> getBaselineRetriever( Set<Feature> features,
                                                                      TimeWindowOuter timeWindow )
    {
        Supplier<Stream<TimeSeries<Double>>> baseline = Stream::of;

        if ( this.hasBaseline() )
        {
            LOGGER.debug( "Creating a baseline retriever for project '{}', features '{}' and time window {}.",
                          this.project.getId(),
                          features,
                          timeWindow );

            baseline = this.getSingleValuedRetrieverBuilder( this.baselineDataset.type() )
                           .setDatabase( this.getDatabase() )
                           .setFeaturesCache( this.getFeaturesCache() )
                           .setMeasurementUnitsCache( this.getMeasurementUnitsCache() )
                           .setProjectId( this.project.getId() )
                           .setFeatures( features )
                           .setVariableName( this.project.getBaselineVariable()
                                                         .name() )
                           .setDatasetOrientation( DatasetOrientation.BASELINE )
                           .setDeclaredExistingTimeScale( this.getDeclaredExistingTimeScale( this.baselineDataset ) )
                           .setDesiredTimeScale( this.desiredTimeScale )
                           .setSeasonStart( this.seasonStart )
                           .setSeasonEnd( this.seasonEnd )
                           .setTimeWindow( timeWindow )
                           .build();
        }

        return baseline;
    }

    @Override
    public Supplier<Stream<TimeSeries<Double>>> getCovariateRetriever( Set<Feature> features, String variableName )
    {
        return this.leftFactory.getCovariateRetriever( features, variableName );

    }

    @Override
    public Supplier<Stream<TimeSeries<Double>>> getCovariateRetriever( Set<Feature> features,
                                                                       String variableName,
                                                                       TimeWindowOuter timeWindow )
    {
        return this.leftFactory.getCovariateRetriever( features, variableName, timeWindow );
    }

    /**
     * Returns <code>true</code> if the project associated with this retriever factory has a baseline, otherwise
     * <code>false</code>.
     *
     * @return true if the project has a baseline, otherwise false
     */

    private boolean hasBaseline()
    {
        return this.project.hasBaseline();
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
     * @return the ensembles cache.
     */

    private Ensembles getEnsemblesCache()
    {
        return this.caches.getEnsemblesCache();
    }

    /**
     * Returns a builder for a retriever.
     *
     * @param dataType the retrieved data type
     * @return the retriever
     * @throws IllegalArgumentException if the data type is unrecognized in this context
     */

    private TimeSeriesRetriever.Builder<Double> getSingleValuedRetrieverBuilder( DataType dataType )
    {
        Duration earliestAnalysisDuration = this.getProject()
                                                .getEarliestAnalysisDuration();
        Duration latestAnalysisDuration = this.getProject()
                                              .getLatestAnalysisDuration();

        return switch ( dataType )
        {
            case SINGLE_VALUED_FORECASTS -> new SingleValuedForecastRetriever.Builder();
            case OBSERVATIONS, SIMULATIONS -> new ObservationRetriever.Builder();
            case ANALYSES -> new AnalysisRetriever.Builder().setEarliestAnalysisDuration( earliestAnalysisDuration )
                                                            .setLatestAnalysisDuration( latestAnalysisDuration )
                                                            .setDuplicatePolicy( DuplicatePolicy.KEEP_LATEST_REFERENCE_TIME );
            default -> throw new IllegalArgumentException( "Unrecognized data type from which to create the "
                                                           + "retriever: "
                                                           + dataType
                                                           + "'." );
        };
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
     * Hidden constructor.
     *
     * @param project the project
     * @param database the database,
     * @param caches the caches
     * @throws NullPointerException if any input is null
     */

    private EnsembleSingleValuedRetrieverFactory( Project project,
                                                  Database database,
                                                  DatabaseCaches caches )
    {
        Objects.requireNonNull( project );
        Objects.requireNonNull( database );
        Objects.requireNonNull( caches );

        this.project = project;
        this.database = database;
        this.caches = caches;

        this.rightDataset = DeclarationUtilities.getDeclaredDataset( project.getDeclaration(),
                                                                     DatasetOrientation.RIGHT );

        if ( project.hasBaseline() )
        {
            this.baselineDataset = DeclarationUtilities.getDeclaredDataset( project.getDeclaration(),
                                                                            DatasetOrientation.BASELINE );
        }
        else
        {
            this.baselineDataset = null;
        }

        // Right data must be ensemble and baseline must be single-valued
        if ( this.rightDataset.type() != DataType.ENSEMBLE_FORECASTS )
        {
            throw new IllegalArgumentException( "When creating a retriever, encountered an unexpected data type. "
                                                + "Expected "
                                                + DataType.ENSEMBLE_FORECASTS
                                                + " for the dataset with orientation "
                                                + DatasetOrientation.RIGHT
                                                + ", but found: "
                                                + this.rightDataset.type()
                                                + "." );
        }
        if ( Objects.nonNull( this.baselineDataset )
             && this.baselineDataset.type() == DataType.ENSEMBLE_FORECASTS )
        {
            throw new IllegalArgumentException( "When creating a retriever, encountered an unexpected data type. "
                                                + "Encountered "
                                                + DataType.ENSEMBLE_FORECASTS
                                                + " for the dataset with orientation "
                                                + DatasetOrientation.BASELINE
                                                + ", but expected one of: "
                                                + Arrays.stream( DataType.values() )
                                                        .filter( n -> n != DataType.ENSEMBLE_FORECASTS )
                                                        .toList() );
        }

        // Obtain any seasonal constraints
        this.seasonStart = project.getStartOfSeason();
        this.seasonEnd = project.getEndOfSeason();

        // Obtain and set the desired timescale.
        this.desiredTimeScale = project.getDesiredTimeScale();

        // Create a factory for the left-ish data
        this.leftFactory = SingleValuedRetrieverFactory.of( project,
                                                            database,
                                                            caches );
    }

}
