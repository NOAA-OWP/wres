package wres.io.retrieval;

import java.sql.SQLException;
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
import wres.datamodel.FeatureTuple;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.time.ReferenceTimeType;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeWindowOuter;
import wres.io.config.ConfigHelper;
import wres.io.data.caching.Features;
import wres.io.project.Project;
import wres.io.retrieval.EnsembleForecastRetriever.Builder;
import wres.io.utilities.Database;

/**
 * <p>A factory class that creates retrievers for the single-valued left and ensemble right datasets associated with one 
 * evaluation. This factory takes a "per-feature-view" of retrieval whereby a feature is supplied on construction. In 
 * future, other implementations may not take a per-feature view (e.g., a multiple-feature-view or a grid-view).
 * 
 * @author james.brown@hydrosolved.com
 */

public class EnsembleRetrieverFactory implements RetrieverFactory<Double, Ensemble>
{

    /**
     * Logger.
     */

    private static final Logger LOGGER = LoggerFactory.getLogger( EnsembleRetrieverFactory.class );

    /**
     * Message about features, re-used several times.
     */
    private static final String FEATURE_MESSAGE = ", feature ";

    /**
     * Message about time windows, re-used several times.
     */

    private static final String AND_TIME_WINDOW_MESSAGE = " and time window ";

    private final Database database;
    private final Features featuresCache;

    /**
     * The project.
     */

    private final Project project;

    /**
     * Right data declaration.
     */

    private final DataSourceConfig rightConfig;

    /**
     * Baseline data declaration.
     */

    private final DataSourceConfig baselineConfig;

    /**
     * Start of a seasonal constraint, if any.
     */

    private final MonthDay seasonStart;

    /**
     * End of a seasonal constraint, if any.
     */

    private final MonthDay seasonEnd;

    /**
     * Declared <code>desiredTimeScale</code>, if any.
     */

    private final TimeScaleOuter desiredTimeScale;

    /**
     * A mapper to convert measurement units.
     */

    private final UnitMapper unitMapper;

    /**
     * A feature tuple for retrieval.
     */

    private final FeatureTuple feature;

    /**
     * A single-valued retriever factory for the left-ish data.
     */

    private final RetrieverFactory<Double,Double> leftFactory;

    private Database getDatabase()
    {
        return this.database;
    }

    private Features getFeaturesCache()
    {
        return this.featuresCache;
    }

    /**
     * Returns an instance.
     *
     * @param database The database to use.
     * @param featuresCache The features cache to use.
     * @param project the project
     * @param feature a feature to evaluate
     * @param unitMapper the unit mapper
     * @return a factory instance
     * @throws NullPointerException if any input is null
     */

    public static EnsembleRetrieverFactory of( Database database,
                                               Features featuresCache,
                                               Project project,
                                               FeatureTuple feature,
                                               UnitMapper unitMapper )
    {
        return new EnsembleRetrieverFactory( database, featuresCache, project, feature, unitMapper );
    }

    @Override
    public Supplier<Stream<TimeSeries<Double>>> getLeftRetriever()
    {
        return this.leftFactory.getLeftRetriever();
    }

    @Override
    public Supplier<Stream<TimeSeries<Double>>> getLeftRetriever( TimeWindowOuter timeWindow )
    {
        return this.leftFactory.getLeftRetriever( timeWindow );
    }

    @Override
    public Supplier<Stream<TimeSeries<Ensemble>>> getRightRetriever( TimeWindowOuter timeWindow )
    {
        LOGGER.debug( "Creating a right retriever for project '{}', feature '{}' and time window {}.",
                      this.project.getId(),
                      this.feature.getRight(),
                      timeWindow );
        try
        {
            // Obtain any ensemble member constraints
            Set<Long> ensembleIdsToInclude =
                    this.project.getEnsembleMembersToFilter( LeftOrRightOrBaseline.RIGHT, true );
            Set<Long> ensembleIdsToExclude =
                    this.project.getEnsembleMembersToFilter( LeftOrRightOrBaseline.RIGHT, false );

            return this.getRightRetrieverBuilder( this.rightConfig.getType() )
                       .setEnsembleIdsToInclude( ensembleIdsToInclude )
                       .setEnsembleIdsToExclude( ensembleIdsToExclude )
                       .setDatabase( this.getDatabase()  )
                       .setFeaturesCache( this.getFeaturesCache() )
                       .setProjectId( this.project.getId() )
                       .setFeature( this.feature.getRight() )
                       .setVariableName( this.project.getRightVariableName() )
                       .setLeftOrRightOrBaseline( LeftOrRightOrBaseline.RIGHT )
                       .setDeclaredExistingTimeScale( this.getDeclaredExistingTimeScale( rightConfig ) )
                       .setDesiredTimeScale( this.desiredTimeScale )
                       .setUnitMapper( this.unitMapper )
                       .setSeasonStart( this.seasonStart )
                       .setSeasonEnd( this.seasonEnd )
                       .setTimeWindow( timeWindow )
                       .build();
        }
        catch ( SQLException e )
        {
            throw new DataAccessException( "While creating a retriever of right data for project "
                                           + this.project.getId()
                                           + FEATURE_MESSAGE
                                           + this.feature.getRight().toString()
                                           + AND_TIME_WINDOW_MESSAGE
                                           + timeWindow
                                           + ":",
                                           e );
        }
    }
    
    @Override
    public Supplier<Stream<TimeSeries<Ensemble>>> getBaselineRetriever()
    {
        return this.getBaselineRetriever( null );
    }

    @Override
    public Supplier<Stream<TimeSeries<Ensemble>>> getBaselineRetriever( TimeWindowOuter timeWindow )
    {
        Supplier<Stream<TimeSeries<Ensemble>>> baseline = null;

        if ( this.hasBaseline() )
        {
            LOGGER.debug( "Creating a baseline retriever for project '{}', feature '{}' and time window {}.",
                          this.project.getId(),
                          this.feature.getBaseline(),
                          timeWindow );
            try
            {
                // Obtain any ensemble member constraints
                Set<Long> ensembleIdsToInclude =
                        this.project.getEnsembleMembersToFilter( LeftOrRightOrBaseline.BASELINE, true );
                Set<Long> ensembleIdsToExclude =
                        this.project.getEnsembleMembersToFilter( LeftOrRightOrBaseline.BASELINE, false );

                baseline = this.getRightRetrieverBuilder( this.baselineConfig.getType() )
                               .setEnsembleIdsToInclude( ensembleIdsToInclude )
                               .setEnsembleIdsToExclude( ensembleIdsToExclude )
                               .setDatabase( this.getDatabase() )
                               .setFeaturesCache( this.getFeaturesCache() )
                               .setProjectId( this.project.getId() )
                               .setFeature( this.feature.getBaseline() )
                               .setVariableName( this.project.getBaselineVariableName() )
                               .setLeftOrRightOrBaseline( LeftOrRightOrBaseline.BASELINE )
                               .setDeclaredExistingTimeScale( this.getDeclaredExistingTimeScale( baselineConfig ) )
                               .setDesiredTimeScale( this.desiredTimeScale )
                               .setUnitMapper( this.unitMapper )
                               .setSeasonStart( this.seasonStart )
                               .setSeasonEnd( this.seasonEnd )
                               .setTimeWindow( timeWindow )
                               .build();
            }
            catch ( SQLException e )
            {
                throw new DataAccessException( "While creating a retriever of right data for project "
                                               + this.project.getId()
                                               + FEATURE_MESSAGE
                                               + this.feature.getRight().toString()
                                               + AND_TIME_WINDOW_MESSAGE
                                               + timeWindow
                                               + ":",
                                               e );
            }
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
     * Hidden constructor.
     * 
     * @param project the project
     * @param feature a feature to evaluate
     * @param unitMapper the unit mapper
     * @throws NullPointerException if any input is null
     */

    private EnsembleRetrieverFactory( Database database,
                                      Features featuresCache,
                                      Project project,
                                      FeatureTuple feature,
                                      UnitMapper unitMapper )
    {
        Objects.requireNonNull( database );
        Objects.requireNonNull( featuresCache );
        Objects.requireNonNull( project );
        Objects.requireNonNull( unitMapper );
        Objects.requireNonNull( feature );

        this.database = database;
        this.featuresCache = featuresCache;
        this.project = project;
        this.feature = feature;
        this.unitMapper = unitMapper;

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
        this.leftFactory = SingleValuedRetrieverFactory.of( database,
                                                            featuresCache,
                                                            project,
                                                            feature,
                                                            unitMapper );
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
            return (Builder) new EnsembleForecastRetriever.Builder().setReferenceTimeType( ReferenceTimeType.T0 );
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

}
