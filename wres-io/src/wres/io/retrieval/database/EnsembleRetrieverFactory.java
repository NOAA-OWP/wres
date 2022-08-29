package wres.io.retrieval.database;

import java.time.MonthDay;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.DatasourceType;
import wres.config.generated.LeftOrRightOrBaseline;
import wres.config.generated.PairConfig;
import wres.config.generated.ProjectConfig;
import wres.config.generated.ProjectConfig.Inputs;
import wres.datamodel.Ensemble;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.FeatureKey;
import wres.datamodel.time.ReferenceTimeType;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeWindowOuter;
import wres.io.config.ConfigHelper;
import wres.io.data.caching.DatabaseCaches;
import wres.io.data.caching.Ensembles;
import wres.io.data.caching.Features;
import wres.io.project.Project;
import wres.io.retrieval.RetrieverFactory;
import wres.io.retrieval.UnitMapper;
import wres.io.utilities.Database;

/**
 * <p>A factory class that creates retrievers for the single-valued left and ensemble right datasets associated with one 
 * evaluation.
 * 
 * @author James Brown
 */

public class EnsembleRetrieverFactory implements RetrieverFactory<Double, Ensemble>
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( EnsembleRetrieverFactory.class );

    /** The project. */
    private final Project project;
    
    /** The database. */
    private final Database database;

    /** The caches/ORMs. */
    private final DatabaseCaches caches;
    
    /** Right data declaration. */
    private final DataSourceConfig rightConfig;

    /** Baseline data declaration. */
    private final DataSourceConfig baselineConfig;

    /** Start of a seasonal constraint, if any. */
    private final MonthDay seasonStart;

    /** End of a seasonal constraint, if any. */
    private final MonthDay seasonEnd;

    /** Declared <code>desiredTimeScale</code>, if any. */
    private final TimeScaleOuter desiredTimeScale;

    /** A mapper to convert measurement units. */
    private final UnitMapper unitMapper;

    /** A single-valued retriever factory for the left-ish data. */
    private final RetrieverFactory<Double, Double> leftFactory;

    /**
     * Returns an instance.
     *
     * @param project the project
     * @param database the database
     * @param caches the caches
     * @param unitMapper the unit mapper
     * @return a factory instance
     * @throws NullPointerException if any input is null
     */

    public static EnsembleRetrieverFactory of( Project project,
                                               Database database,
                                               DatabaseCaches caches,
                                               UnitMapper unitMapper )
    {
        return new EnsembleRetrieverFactory( project, database, caches, unitMapper );
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
        return this.leftFactory.getLeftRetriever( features );
    }

    @Override
    public Supplier<Stream<TimeSeries<Double>>> getLeftRetriever( Set<FeatureKey> features,
                                                                  TimeWindowOuter timeWindow )
    {
        return this.leftFactory.getLeftRetriever( features, timeWindow );
    }

    @Override
    public Supplier<Stream<TimeSeries<Ensemble>>> getRightRetriever( Set<FeatureKey> features,
                                                                     TimeWindowOuter timeWindow )
    {
        LOGGER.debug( "Creating a right retriever for project '{}', features '{}' and time window {}.",
                      this.project.getId(),
                      features,
                      timeWindow );

        return this.getRightRetrieverBuilder( this.rightConfig.getType() )
                   .setEnsemblesCache( this.getEnsemblesCache() )
                   .setDatabase( this.getDatabase() )
                   .setFeaturesCache( this.getFeaturesCache() )
                   .setProjectId( this.project.getId() )
                   .setFeatures( features )
                   .setVariableName( this.project.getVariableName( LeftOrRightOrBaseline.RIGHT ) )
                   .setLeftOrRightOrBaseline( LeftOrRightOrBaseline.RIGHT )
                   .setDeclaredExistingTimeScale( this.getDeclaredExistingTimeScale( rightConfig ) )
                   .setDesiredTimeScale( this.desiredTimeScale )
                   .setUnitMapper( this.unitMapper )
                   .setSeasonStart( this.seasonStart )
                   .setSeasonEnd( this.seasonEnd )
                   .setTimeWindow( timeWindow )
                   .build();
    }

    @Override
    public Supplier<Stream<TimeSeries<Ensemble>>> getBaselineRetriever( Set<FeatureKey> features )
    {
        return this.getBaselineRetriever( features, null );
    }

    @Override
    public Supplier<Stream<TimeSeries<Ensemble>>> getBaselineRetriever( Set<FeatureKey> features, 
                                                                        TimeWindowOuter timeWindow )
    {
        Supplier<Stream<TimeSeries<Ensemble>>> baseline = Stream::of;

        if ( this.hasBaseline() )
        {
            LOGGER.debug( "Creating a baseline retriever for project '{}', features '{}' and time window {}.",
                          this.project.getId(),
                          features,
                          timeWindow );

            baseline = this.getRightRetrieverBuilder( this.baselineConfig.getType() )
                           .setEnsemblesCache( this.getEnsemblesCache() )
                           .setDatabase( this.getDatabase() )
                           .setFeaturesCache( this.getFeaturesCache() )
                           .setProjectId( this.project.getId() )
                           .setFeatures( features )
                           .setVariableName( this.project.getVariableName( LeftOrRightOrBaseline.BASELINE ) )
                           .setLeftOrRightOrBaseline( LeftOrRightOrBaseline.BASELINE )
                           .setDeclaredExistingTimeScale( this.getDeclaredExistingTimeScale( baselineConfig ) )
                           .setDesiredTimeScale( this.desiredTimeScale )
                           .setUnitMapper( this.unitMapper )
                           .setSeasonStart( this.seasonStart )
                           .setSeasonEnd( this.seasonEnd )
                           .setTimeWindow( timeWindow )
                           .build();
        }

        return baseline;
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
     * @return the ensembles cache.
     */

    private Ensembles getEnsemblesCache()
    {
        return this.caches.getEnsemblesCache();
    }

    /**
     * Returns a builder for a right-ish retriever.
     * 
     * @param dataType the retrieved data type
     * @return the retriever
     * @throws IllegalArgumentException if the data type is unrecognized in this context
     */

    private EnsembleForecastRetriever.Builder getRightRetrieverBuilder( DatasourceType dataType )
    {
        if ( dataType == DatasourceType.ENSEMBLE_FORECASTS )
        {
            return (EnsembleForecastRetriever.Builder) new EnsembleForecastRetriever.Builder().setReferenceTimeType( ReferenceTimeType.T0 );
        }
        else
        {
            throw new IllegalArgumentException( "Unrecognized data type from which to create the ensemble "
                                                + "retriever: "
                                                + dataType
                                                + "'." );
        }
    }

    /**
     * Returns the declared existing time scale associated with a data source, if any.
     * 
     * @param dataSourceConfig the data source declaration
     * @return a declared existing time scale, or null
     */

    private TimeScaleOuter getDeclaredExistingTimeScale( DataSourceConfig dataSourceConfig )
    {
        // Declared existing scale, which can be used to augment a source
        TimeScaleOuter declaredExistingTimeScale = null;

        if ( Objects.nonNull( dataSourceConfig.getExistingTimeScale() ) )
        {
            declaredExistingTimeScale = TimeScaleOuter.of( dataSourceConfig.getExistingTimeScale() );
        }

        return declaredExistingTimeScale;
    }

    /**
     * Hidden constructor.
     * 
     * @param project the project
     * @param database the database,
     * @param caches the caches
     * @param unitMapper the unit mapper
     * @throws NullPointerException if any input is null
     */

    private EnsembleRetrieverFactory( Project project,
                                      Database database,
                                      DatabaseCaches caches,
                                      UnitMapper unitMapper )
    {
        Objects.requireNonNull( project );
        Objects.requireNonNull( unitMapper );
        Objects.requireNonNull( database );
        Objects.requireNonNull( caches );

        this.project = project;
        this.unitMapper = unitMapper;
        this.database = database;
        this.caches = caches;

        ProjectConfig projectConfig = project.getProjectConfig();
        PairConfig pairConfig = projectConfig.getPair();
        Inputs inputsConfig = projectConfig.getInputs();
        this.rightConfig = inputsConfig.getRight();
        this.baselineConfig = inputsConfig.getBaseline();

        // Obtain any seasonal constraints
        this.seasonStart = project.getEarliestDayInSeason();
        this.seasonEnd = project.getLatestDayInSeason();

        // Obtain and set the desired time scale. 
        this.desiredTimeScale = ConfigHelper.getDesiredTimeScale( pairConfig );

        // Create a factory for the left-ish data
        this.leftFactory = SingleValuedRetrieverFactory.of( project,
                                                            database,
                                                            caches,
                                                            unitMapper );
    }

}
