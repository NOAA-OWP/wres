package wres.io.retrieval;

import java.time.MonthDay;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.DatasourceType;
import wres.config.generated.EnsembleCondition;
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
import wres.io.data.caching.Ensembles;
import wres.io.data.caching.Features;
import wres.io.project.Project;
import wres.io.utilities.Database;

/**
 * <p>A factory class that creates retrievers for the single-valued left and ensemble right datasets associated with one 
 * evaluation. This factory takes a "per-feature-view" of retrieval whereby a feature is supplied on construction. In 
 * future, other implementations may not take a per-feature view (e.g., a multiple-feature-view or a grid-view).
 * 
 * @author James Brown
 */

public class EnsembleRetrieverFactory implements RetrieverFactory<Double, Ensemble>
{

    /**
     * Logger.
     */

    private static final Logger LOGGER = LoggerFactory.getLogger( EnsembleRetrieverFactory.class );

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
     * A single-valued retriever factory for the left-ish data.
     */

    private final RetrieverFactory<Double, Double> leftFactory;

    /**
     * Returns an instance.
     *
     * @param project the project
     * @param unitMapper the unit mapper
     * @return a factory instance
     * @throws NullPointerException if any input is null
     */

    public static EnsembleRetrieverFactory of( Project project,
                                               UnitMapper unitMapper )
    {
        return new EnsembleRetrieverFactory( project, unitMapper );
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

        // Obtain any ensemble member constraints
        Set<Long> ensembleIdsToInclude =
                this.getEnsembleMembersToFilter( LeftOrRightOrBaseline.RIGHT, true );
        Set<Long> ensembleIdsToExclude =
                this.getEnsembleMembersToFilter( LeftOrRightOrBaseline.RIGHT, false );

        return this.getRightRetrieverBuilder( this.rightConfig.getType() )
                   .setEnsembleIdsToInclude( ensembleIdsToInclude )
                   .setEnsembleIdsToExclude( ensembleIdsToExclude )
                   .setEnsemblesCache( this.getEnsemblesCache() )
                   .setDatabase( this.getDatabase() )
                   .setFeaturesCache( this.getFeaturesCache() )
                   .setProjectId( this.project.getId() )
                   .setFeatures( features )
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

    @Override
    public Supplier<Stream<TimeSeries<Ensemble>>> getBaselineRetriever( Set<FeatureKey> features )
    {
        return this.getBaselineRetriever( features );
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

            // Obtain any ensemble member constraints
            Set<Long> ensembleIdsToInclude =
                    this.getEnsembleMembersToFilter( LeftOrRightOrBaseline.BASELINE, true );
            Set<Long> ensembleIdsToExclude =
                    this.getEnsembleMembersToFilter( LeftOrRightOrBaseline.BASELINE, false );

            baseline = this.getRightRetrieverBuilder( this.baselineConfig.getType() )
                           .setEnsembleIdsToInclude( ensembleIdsToInclude )
                           .setEnsembleIdsToExclude( ensembleIdsToExclude )
                           .setEnsemblesCache( this.getEnsemblesCache() )
                           .setDatabase( this.getDatabase() )
                           .setFeaturesCache( this.getFeaturesCache() )
                           .setProjectId( this.project.getId() )
                           .setFeatures( features )
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
        return this.project.getDatabase();
    }

    /**
     * @return the features cache.
     */

    private Features getFeaturesCache()
    {
        return this.project.getFeaturesCache();
    }

    /**
     * @return the ensembles cache.
     */

    private Ensembles getEnsemblesCache()
    {
        return this.project.getEnsemblesCache();
    }

    /**
     * @return the project declaration.
     */

    private ProjectConfig getProjectConfig()
    {
        return this.project.getProjectConfig();
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
     * Returns a set of <code>ensemble_id</code> that should be included or excluded. The empty set should be 
     * interpreted as no constraints existing and, hence, that all possible <code>ensemble_id</code> should be included.
     * Including nothing is exceptional. Excluding everything is not exceptional at this stage, but should be 
     * exceptional downstream.
     * 
     * @param dataType the data type
     * @param include is true to search for constraints to include, false to exclude
     * @return the members to include or execlude
     * @throws NullPointerException if the dataType is null
     * @throws IllegalArgumentException if the dataType declaration is invalid or one or more constraints were present 
     *            but not recognized
     */

    private Set<Long> getEnsembleMembersToFilter( LeftOrRightOrBaseline dataType, boolean include )
    {
        Objects.requireNonNull( dataType );

        DataSourceConfig config = null;
        switch ( dataType )
        {
            case LEFT:
                config = this.getProjectConfig().getInputs().getLeft();
                break;
            case RIGHT:
                config = this.getProjectConfig().getInputs().getRight();
                break;
            case BASELINE:
                config = this.getProjectConfig().getInputs().getBaseline();
                break;
            default:
                throw new IllegalArgumentException( "Unrecognized data type '" + dataType + "'." );

        }

        Set<Long> returnMe = new TreeSet<>();

        if ( Objects.nonNull( config ) && !config.getEnsemble().isEmpty() )
        {
            List<EnsembleCondition> conditions = config.getEnsemble();
            Ensembles ensemblesCache = this.getEnsemblesCache();
            List<String> failed = new ArrayList<>();

            for ( EnsembleCondition condition : conditions )
            {
                if ( condition.isExclude() != include )
                {
                    List<Long> filtered = ensemblesCache.getEnsembleIDs( condition );
                    returnMe.addAll( filtered );
                    if ( filtered.isEmpty() && include ) // Do not allow "include nothing"
                    {
                        failed.add( condition.getName() );
                    }
                }
            }

            if ( !failed.isEmpty() )
            {
                throw new IllegalArgumentException( "Of the filters that were defined for ensemble names, "
                                                    + failed.size()
                                                    + " of those filters contained an ensemble name that was not "
                                                    + "present anywhere in the dataset and the filter asked for the "
                                                    + "name to be included. Inclusive filters can only be defined "
                                                    + "for ensemble names that exist. Fix the declared filters to "
                                                    + "reference names that exist. The ensemble names that do not "
                                                    + "exist are: "
                                                    + failed
                                                    + "." );
            }
        }

        return Collections.unmodifiableSet( returnMe );
    }

    /**
     * Hidden constructor.
     * 
     * @param project the project
     * @param unitMapper the unit mapper
     * @throws NullPointerException if any input is null
     */

    private EnsembleRetrieverFactory( Project project,
                                      UnitMapper unitMapper )
    {
        Objects.requireNonNull( project );
        Objects.requireNonNull( unitMapper );

        this.project = project;
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
        this.leftFactory = SingleValuedRetrieverFactory.of( project,
                                                            unitMapper );
    }

}
