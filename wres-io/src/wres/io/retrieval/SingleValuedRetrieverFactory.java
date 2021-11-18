package wres.io.retrieval;

import java.sql.SQLException;
import java.time.Duration;
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
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.FeatureKey;
import wres.datamodel.time.ReferenceTimeType;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeWindowOuter;
import wres.io.config.ConfigHelper;
import wres.io.data.caching.Features;
import wres.io.project.Project;
import wres.io.retrieval.AnalysisRetriever.DuplicatePolicy;
import wres.io.retrieval.SingleValuedGriddedRetriever.Builder;
import wres.io.retrieval.TimeSeriesRetriever.TimeSeriesRetrieverBuilder;
import wres.io.utilities.Database;

/**
 * <p>A factory class that creates retrievers for the single-valued left and right datasets associated with one 
 * evaluation. This factory takes a "per-feature-view" of retrieval whereby a feature is supplied on construction. In 
 * future, other implementations may not take a per-feature view (e.g., a multiple-feature-view or a grid-view).
 * 
 * @author james.brown@hydrosolved.com
 */

public class SingleValuedRetrieverFactory implements RetrieverFactory<Double, Double>
{

    /**
     * Logger.
     */

    private static final Logger LOGGER = LoggerFactory.getLogger( SingleValuedRetrieverFactory.class );

    /**
     * Message about features, re-used several times.
     */
    private static final String FEATURE_MESSAGE = ", features ";

    /**
     * Message about time windows, re-used several times.
     */

    private static final String AND_TIME_WINDOW_MESSAGE = " and time window ";

    /**
     * The project.
     */

    private final Project project;

    /**
     * Left data declaration.
     */

    private final DataSourceConfig leftConfig;

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
     * Returns an instance.
     *
     * @param project the project
     * @param unitMapper the unit mapper
     * @return a factory instance
     * @throws NullPointerException if any input is null
     */

    public static SingleValuedRetrieverFactory of( Project project,
                                                   UnitMapper unitMapper )
    {
        return new SingleValuedRetrieverFactory( project,
                                                 unitMapper );
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
        return this.get( this.leftConfig,
                         features,
                         null );
    }

    @Override
    public Supplier<Stream<TimeSeries<Double>>> getLeftRetriever( Set<FeatureKey> features,
                                                                  TimeWindowOuter timeWindow )
    {
        return this.get( this.leftConfig,
                         features,
                         timeWindow );
    }

    @Override
    public Supplier<Stream<TimeSeries<Double>>> getRightRetriever( Set<FeatureKey> features,
                                                                   TimeWindowOuter timeWindow )
    {
        return this.get( this.rightConfig,
                         features,
                         timeWindow );
    }

    @Override
    public Supplier<Stream<TimeSeries<Double>>> getBaselineRetriever( Set<FeatureKey> features )
    {
        return this.get( this.baselineConfig,
                         features,
                         null );
    }

    @Override
    public Supplier<Stream<TimeSeries<Double>>> getBaselineRetriever( Set<FeatureKey> features,
                                                                      TimeWindowOuter timeWindow )
    {
        return this.get( this.baselineConfig,
                         features,
                         timeWindow );
    }

    /**
     * Returns a supplier of time-series.
     * 
     * @param dataSourceConfig the data source configuration
     * @param features the features
     * @param timeWindow the time window
     * @param featureName the feature name
     * @return the supplier
     */

    private Supplier<Stream<TimeSeries<Double>>> get( DataSourceConfig dataSourceConfig,
                                                      Set<FeatureKey> features,
                                                      TimeWindowOuter timeWindow )
    {
        Objects.requireNonNull( dataSourceConfig );
        Objects.requireNonNull( features );
        ProjectConfig projectConfig = this.getProject()
                                          .getProjectConfig();

        LeftOrRightOrBaseline leftOrRightOrBaseline =
                ConfigHelper.getLeftOrRightOrBaseline( projectConfig,
                                                       dataSourceConfig );
        LOGGER.debug( "Creating a {} retriever for project '{}', features '{}' and time window {}.",
                      leftOrRightOrBaseline,
                      this.getProject().getId(),
                      features,
                      timeWindow );
        TimeSeriesRetrieverBuilder<Double> builder;

        boolean isConfiguredAsForecast = ConfigHelper.isForecast( dataSourceConfig );
        String variableName = this.getProject()
                                  .getVariableName( leftOrRightOrBaseline );
        TimeScaleOuter declaredExistingTimeScale =
                this.getDeclaredExistingTimeScale( dataSourceConfig );

        try
        {
            // Gridded data?
            if ( this.getProject().usesGriddedData( dataSourceConfig ) )
            {
                builder = this.getGriddedRetrieverBuilder( dataSourceConfig.getType() )
                              .setIsForecast( isConfiguredAsForecast )
                              .setFeatures( features );
            }
            else
            {
                builder = this.getRetrieverBuilder( dataSourceConfig.getType() )
                              .setFeatures( features );
            }
        }
        catch ( SQLException e )
        {
            throw new DataAccessException( "While creating a retriever of "
                                           + leftOrRightOrBaseline
                                           + " data for project "
                                           + this.getProject().getId()
                                           + FEATURE_MESSAGE
                                           + features
                                           + AND_TIME_WINDOW_MESSAGE
                                           + timeWindow
                                           + ":",
                                           e );
        }

        builder.setDatabase( this.getDatabase() )
               .setFeaturesCache( this.getFeaturesCache() )
               .setProjectId( this.getProject().getId() )
               .setVariableName( variableName )
               .setLeftOrRightOrBaseline( leftOrRightOrBaseline )
               .setDeclaredExistingTimeScale( declaredExistingTimeScale )
               .setDesiredTimeScale( this.desiredTimeScale )
               .setUnitMapper( this.unitMapper );

        if ( Objects.nonNull( timeWindow ) )
        {
            builder.setTimeWindow( timeWindow );
        }

        // TODO: reconsider how seasons are applied. For now, do not apply to
        // left-ish data because the current interpretation of right-ish data
        // with a forecast type is to use reference time, not valid time. See #40405

        if ( !leftOrRightOrBaseline.equals( LeftOrRightOrBaseline.LEFT ) )
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
     * Returns a builder for a retriever.
     * 
     * @param dataType the retrieved data type
     * @return the retriever
     * @throws IllegalArgumentException if the data type is unrecognized in this context
     */

    private TimeSeriesRetrieverBuilder<Double> getRetrieverBuilder( DatasourceType dataType )
    {

        Duration earliestAnalysisDuration = this.getProject()
                                                .getEarliestAnalysisDurationOrNull();
        Duration latestAnalysisDuration = this.getProject()
                                              .getLatestAnalysisDurationOrNull();

        switch ( dataType )
        {
            case SINGLE_VALUED_FORECASTS:
                return new SingleValuedForecastRetriever.Builder().setReferenceTimeType( ReferenceTimeType.T0 );
            case OBSERVATIONS:
                return new ObservationRetriever.Builder();
            case SIMULATIONS:
                return new ObservationRetriever.Builder().setReferenceTimeType( ReferenceTimeType.ANALYSIS_START_TIME );
            case ANALYSES:
                return new AnalysisRetriever.Builder().setEarliestAnalysisDuration( earliestAnalysisDuration )
                                                      .setLatestAnalysisDuration( latestAnalysisDuration )
                                                      .setDuplicatePolicy( DuplicatePolicy.KEEP_LATEST_REFERENCE_TIME )
                                                      .setReferenceTimeType( ReferenceTimeType.ANALYSIS_START_TIME );
            default:
                throw new IllegalArgumentException( "Unrecognized data type from which to create the single-valued "
                                                    + "retriever: "
                                                    + dataType
                                                    + "'." );
        }
    }

    /**
     * Returns a builder for a gridded retriever.
     * 
     * @param dataType the retrieved data type
     * @return the retriever
     * @throws IllegalArgumentException if the data type is unrecognized in this context
     */

    private SingleValuedGriddedRetriever.Builder getGriddedRetrieverBuilder( DatasourceType dataType )
    {
        switch ( dataType )
        {
            case SINGLE_VALUED_FORECASTS:
                return (Builder) new SingleValuedGriddedRetriever.Builder().setIsForecast( true )
                                                                           .setReferenceTimeType( ReferenceTimeType.T0 );
            case OBSERVATIONS:
            case SIMULATIONS:
                return (Builder) new SingleValuedGriddedRetriever.Builder().setReferenceTimeType( ReferenceTimeType.ANALYSIS_START_TIME );
            default:
                throw new IllegalArgumentException( "Unrecognized data type from which to create the single-valued "
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
     * @param unitMapper the unit mapper
     * @throws NullPointerException if any input is null
     */

    private SingleValuedRetrieverFactory( Project project,
                                          UnitMapper unitMapper )
    {
        Objects.requireNonNull( project );
        Objects.requireNonNull( unitMapper );

        this.project = project;
        this.unitMapper = unitMapper;

        ProjectConfig projectConfig = project.getProjectConfig();
        PairConfig pairConfig = projectConfig.getPair();
        Inputs inputsConfig = projectConfig.getInputs();
        this.leftConfig = inputsConfig.getLeft();
        this.rightConfig = inputsConfig.getRight();
        this.baselineConfig = inputsConfig.getBaseline();

        // Obtain any seasonal constraints
        this.seasonStart = project.getEarliestDayInSeason();
        this.seasonEnd = project.getLatestDayInSeason();

        // Obtain and set the desired time scale. 
        this.desiredTimeScale = ConfigHelper.getDesiredTimeScale( pairConfig );
    }

}
