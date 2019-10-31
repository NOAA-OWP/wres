package wres.io.retrieval.datashop;

import java.sql.SQLException;
import java.time.MonthDay;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.DatasourceType;
import wres.config.generated.Feature;
import wres.config.generated.PairConfig;
import wres.config.generated.ProjectConfig;
import wres.config.generated.ProjectConfig.Inputs;
import wres.datamodel.scale.TimeScale;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeWindow;
import wres.io.config.ConfigHelper;
import wres.io.config.LeftOrRightOrBaseline;
import wres.io.project.Project;
import wres.io.retrieval.datashop.TimeSeriesRetriever.TimeSeriesRetrieverBuilder;

/**
 * <p>A factory class that creates retrievers for the single-valued left and right datasets associated with one 
 * evaluation. This factory takes a "per-feature-view" of retrieval whereby a feature is supplied on construction. In 
 * future, other implementations may not take a per-feature view (e.g., a multiple-feature-view or a grid-view).
 * 
 * @author james.brown@hydrosolved.com
 */

class SingleValuedRetrieverFactory implements RetrieverFactory<Double, Double>
{

    /**
     * Logger.
     */

    private static final Logger LOGGER = LoggerFactory.getLogger( SingleValuedRetrieverFactory.class );

    /**
     * Message about features, re-used several times.
     */
    private static final String FEATURE_MESSAGE = ", feature ";

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

    private final TimeScale desiredTimeScale;

    /**
     * A mapper to convert measurement units.
     */

    private final UnitMapper unitMapper;

    /**
     * A feature for retrieval.
     */

    private final Feature feature;

    /**
     * A string that describes the feature.
     */

    private final String featureString;

    /**
     * Returns an instance.
     * 
     * @param project the project
     * @param feature a feature to evaluate
     * @param unitMapper the unit mapper
     * @throws NullPointerException if any input is null
     */

    static SingleValuedRetrieverFactory of( Project project, Feature feature, UnitMapper unitMapper )
    {
        return new SingleValuedRetrieverFactory( project, feature, unitMapper );
    }

    @Override
    public Supplier<Stream<TimeSeries<Double>>> getLeftRetriever()
    {
        LOGGER.debug( "Creating a left retriever for project '{}' and feature '{}'.",
                      this.project.getId(),
                      this.featureString );
        try
        {
            TimeSeriesRetrieverBuilder<Double> builder = null;

            // Gridded data?
            if ( this.project.usesGriddedData( this.leftConfig ) )
            {
                builder = this.getGriddedRetrieverBuilder( this.leftConfig.getType() )
                              .setVariableName( this.leftConfig.getVariable().getValue() )
                              .setFeatures( List.of( this.feature ) );
            }
            else
            {
                int leftVariableFeatureId = project.getLeftVariableFeatureId( this.feature );
                builder = this.getRetrieverBuilder( this.leftConfig.getType() )
                              .setVariableFeatureId( leftVariableFeatureId );
            }

            // TODO: reconsider how seasons are applied. For now, do not apply to
            // left-ish data because the current interpretation of right-ish data
            // with a forecast type is to use reference time, not valid time. See #40405
            return builder.setProjectId( this.project.getId() )
                          .setLeftOrRightOrBaseline( LeftOrRightOrBaseline.LEFT )
                          .setDeclaredExistingTimeScale( this.getDeclaredExistingTimeScale( this.leftConfig ) )
                          .setDesiredTimeScale( this.desiredTimeScale )
                          //.setSeasonStart( this.seasonStart )
                          //.setSeasonEnd( this.seasonEnd )
                          .setUnitMapper( this.unitMapper )
                          .build();
        }
        catch ( SQLException e )
        {
            throw new DataAccessException( "While creating a retriever of left data for project "
                                           + this.project.getId()
                                           + " and feature "
                                           + this.featureString
                                           + ":",
                                           e );
        }
    }

    @Override
    public Supplier<Stream<TimeSeries<Double>>> getLeftRetriever( TimeWindow timeWindow )
    {
        LOGGER.debug( "Creating a left retriever for project '{}', feature '{}' and time window {}.",
                      this.project.getId(),
                      this.featureString,
                      timeWindow );
        try
        {
            TimeSeriesRetrieverBuilder<Double> builder = null;

            // Gridded data?
            if ( this.project.usesGriddedData( this.leftConfig ) )
            {
                builder = this.getGriddedRetrieverBuilder( this.leftConfig.getType() )
                              .setVariableName( this.leftConfig.getVariable().getValue() )
                              .setFeatures( List.of( this.feature ) );
            }
            else
            {
                int leftVariableFeatureId = project.getLeftVariableFeatureId( this.feature );
                builder = this.getRetrieverBuilder( this.leftConfig.getType() )
                              .setVariableFeatureId( leftVariableFeatureId );
            }

            // TODO: reconsider how seasons are applied. For now, do not apply to
            // left-ish data because the current interpretation of right-ish data
            // with a forecast type is to use reference time, not valid time. See #40405
            return builder.setProjectId( this.project.getId() )
                          .setLeftOrRightOrBaseline( LeftOrRightOrBaseline.LEFT )
                          .setDeclaredExistingTimeScale( this.getDeclaredExistingTimeScale( this.leftConfig ) )
                          .setDesiredTimeScale( this.desiredTimeScale )
                          //.setSeasonStart( this.seasonStart )
                          //.setSeasonEnd( this.seasonEnd )
                          .setUnitMapper( this.unitMapper )
                          .setTimeWindow( timeWindow )
                          .build();
        }
        catch ( SQLException e )
        {
            throw new DataAccessException( "While creating a retriever of left data for project "
                                           + this.project.getId()
                                           + FEATURE_MESSAGE
                                           + this.featureString
                                           + AND_TIME_WINDOW_MESSAGE
                                           + timeWindow
                                           + ":",
                                           e );
        }
    }

    @Override
    public Supplier<Stream<TimeSeries<Double>>> getRightRetriever( TimeWindow timeWindow )
    {
        LOGGER.debug( "Creating a right retriever for project '{}', feature '{}' and time window {}.",
                      this.project.getId(),
                      this.featureString,
                      timeWindow );
        try
        {
            TimeSeriesRetrieverBuilder<Double> builder = null;

            // Gridded data?
            if ( this.project.usesGriddedData( this.rightConfig ) )
            {
                builder = this.getGriddedRetrieverBuilder( this.rightConfig.getType() )
                              .setVariableName( this.rightConfig.getVariable().getValue() )
                              .setFeatures( List.of( this.feature ) );
            }
            else
            {
                int rightVariableFeatureId = project.getRightVariableFeatureId( this.feature );
                builder = this.getRetrieverBuilder( this.rightConfig.getType() )
                              .setVariableFeatureId( rightVariableFeatureId );
            }

            // TODO: reconsider how seasons are applied. For now, do not apply to
            // left-ish data because the current interpretation of right-ish data
            // with a forecast type is to use reference time, not valid time. See #40405
            return builder.setProjectId( this.project.getId() )
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
                                           + this.featureString
                                           + AND_TIME_WINDOW_MESSAGE
                                           + timeWindow
                                           + ":",
                                           e );
        }
    }

    @Override
    public Supplier<Stream<TimeSeries<Double>>> getBaselineRetriever( TimeWindow timeWindow )
    {
        Supplier<Stream<TimeSeries<Double>>> baseline = null;

        if ( this.hasBaseline() )
        {
            LOGGER.debug( "Creating a baseline retriever for project '{}', feature '{}' and time window {}.",
                          this.project.getId(),
                          this.featureString,
                          timeWindow );
            try
            {
                TimeSeriesRetrieverBuilder<Double> builder = null;

                // Gridded data?
                if ( this.project.usesGriddedData( this.baselineConfig ) )
                {
                    builder = this.getGriddedRetrieverBuilder( this.baselineConfig.getType() )
                                  .setVariableName( this.baselineConfig.getVariable().getValue() )
                                  .setFeatures( List.of( this.feature ) );
                }
                else
                {
                    int baselineVariableFeatureId = project.getBaselineVariableFeatureId( feature );
                    builder = this.getRetrieverBuilder( this.baselineConfig.getType() )
                                  .setVariableFeatureId( baselineVariableFeatureId );
                }

                // TODO: reconsider how seasons are applied. For now, do not apply to
                // left-ish data because the current interpretation of right-ish data
                // with a forecast type is to use reference time, not valid time. See #40405
                return builder.setProjectId( this.project.getId() )
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
                                               + this.featureString
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
     * @param feature a feature
     * @param unitMapper the unit mapper
     * @throws NullPointerException if any input is null
     */

    private SingleValuedRetrieverFactory( Project project, Feature feature, UnitMapper unitMapper )
    {
        Objects.requireNonNull( project );
        Objects.requireNonNull( unitMapper );
        Objects.requireNonNull( feature );

        this.project = project;
        this.feature = feature;
        this.unitMapper = unitMapper;

        this.featureString = ConfigHelper.getFeatureDescription( feature );

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

    /**
     * Returns a builder for a retriever.
     * 
     * @param dataType the retrieved data type
     * @return the retriever
     * @throws IllegalArgumentException if the data type is unrecognized in this context
     */

    private TimeSeriesRetrieverBuilder<Double> getRetrieverBuilder( DatasourceType dataType )
    {
        switch ( dataType )
        {
            case SINGLE_VALUED_FORECASTS:
                return new SingleValuedForecastRetriever.Builder();
            case OBSERVATIONS:
            case SIMULATIONS:
                return new ObservationRetriever.Builder();
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
                return new SingleValuedGriddedRetriever.Builder().setIsForecast( true );
            case OBSERVATIONS:
            case SIMULATIONS:
                return new SingleValuedGriddedRetriever.Builder();
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

    private TimeScale getDeclaredExistingTimeScale( DataSourceConfig dataSourceConfig )
    {
        // Declared existing scale, which can be used to augment a source
        TimeScale declaredExistingTimeScale = null;

        if ( Objects.nonNull( dataSourceConfig.getExistingTimeScale() ) )
        {
            declaredExistingTimeScale = TimeScale.of( dataSourceConfig.getExistingTimeScale() );
        }

        return declaredExistingTimeScale;
    }

}
