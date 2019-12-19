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
import wres.config.generated.Feature;
import wres.config.generated.PairConfig;
import wres.config.generated.ProjectConfig;
import wres.config.generated.ProjectConfig.Inputs;
import wres.datamodel.Ensemble;
import wres.datamodel.scale.TimeScale;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeWindow;
import wres.io.config.ConfigHelper;
import wres.io.config.LeftOrRightOrBaseline;
import wres.io.project.Project;

/**
 * <p>A factory class that creates retrievers for the single-valued left and ensemble right datasets associated with one 
 * evaluation. This factory takes a "per-feature-view" of retrieval whereby a feature is supplied on construction. In 
 * future, other implementations may not take a per-feature view (e.g., a multiple-feature-view or a grid-view).
 * 
 * @author james.brown@hydrosolved.com
 */

class EnsembleRetrieverFactory implements RetrieverFactory<Double, Ensemble>
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
     * A single-valued retriever factory for the left-ish data.
     */

    private final RetrieverFactory<Double,Double> leftFactory;
    
    /**
     * Returns an instance.
     * 
     * @param project the project
     * @param feature a feature to evaluate
     * @param unitMapper the unit mapper
     * @throws NullPointerException if any input is null
     */

    static EnsembleRetrieverFactory of( Project project, Feature feature, UnitMapper unitMapper )
    {
        return new EnsembleRetrieverFactory( project, feature, unitMapper );
    }

    @Override
    public Supplier<Stream<TimeSeries<Double>>> getLeftRetriever()
    {
        return this.leftFactory.getLeftRetriever();
    }

    @Override
    public Supplier<Stream<TimeSeries<Double>>> getLeftRetriever( TimeWindow timeWindow )
    {
        return this.leftFactory.getLeftRetriever( timeWindow );
    }

    @Override
    public Supplier<Stream<TimeSeries<Ensemble>>> getRightRetriever( TimeWindow timeWindow )
    {
        LOGGER.debug( "Creating a right retriever for project '{}', feature '{}' and time window {}.",
                      this.project.getId(),
                      this.featureString,
                      timeWindow );
        try
        {
            int rightVariableFeatureId = project.getRightVariableFeatureId( this.feature );

            // Many sources per time-series?
            boolean hasMultipleSourcesPerSeries =
                    ConfigHelper.hasSourceFormatWithMultipleSourcesPerSeries( this.rightConfig );

            // Obtain any ensemble member constraints
            Set<Integer> ensembleIdsToInclude =
                    this.project.getEnsembleMembersToFilter( LeftOrRightOrBaseline.RIGHT, true );
            Set<Integer> ensembleIdsToExclude =
                    this.project.getEnsembleMembersToFilter( LeftOrRightOrBaseline.RIGHT, false );

            return this.getRightRetrieverBuilder( this.rightConfig.getType() )
                       .setEnsembleIdsToInclude( ensembleIdsToInclude )
                       .setEnsembleIdsToExclude( ensembleIdsToExclude )
                       .setHasMultipleSourcesPerSeries( hasMultipleSourcesPerSeries )
                       .setProjectId( this.project.getId() )
                       .setVariableFeatureId( rightVariableFeatureId )
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
    public Supplier<Stream<TimeSeries<Ensemble>>> getBaselineRetriever( TimeWindow timeWindow )
    {
        Supplier<Stream<TimeSeries<Ensemble>>> baseline = null;

        if ( this.hasBaseline() )
        {
            LOGGER.debug( "Creating a baseline retriever for project '{}', feature '{}' and time window {}.",
                          this.project.getId(),
                          this.featureString,
                          timeWindow );
            try
            {

                int baselineVariableFeatureId = project.getBaselineVariableFeatureId( this.feature );

                // Many sources per time-series?
                boolean hasMultipleSourcesPerSeries =
                        ConfigHelper.hasSourceFormatWithMultipleSourcesPerSeries( this.baselineConfig );

                // Obtain any ensemble member constraints
                Set<Integer> ensembleIdsToInclude =
                        this.project.getEnsembleMembersToFilter( LeftOrRightOrBaseline.BASELINE, true );
                Set<Integer> ensembleIdsToExclude =
                        this.project.getEnsembleMembersToFilter( LeftOrRightOrBaseline.BASELINE, false );

                baseline = this.getRightRetrieverBuilder( this.baselineConfig.getType() )
                               .setEnsembleIdsToInclude( ensembleIdsToInclude )
                               .setEnsembleIdsToExclude( ensembleIdsToExclude )
                               .setHasMultipleSourcesPerSeries( hasMultipleSourcesPerSeries )
                               .setProjectId( this.project.getId() )
                               .setVariableFeatureId( baselineVariableFeatureId )
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
     * @param feature a feature to evaluate
     * @param unitMapper the unit mapper
     * @throws NullPointerException if any input is null
     */

    private EnsembleRetrieverFactory( Project project, Feature feature, UnitMapper unitMapper )
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
        this.rightConfig = inputsConfig.getRight();
        this.baselineConfig = inputsConfig.getBaseline();

        // Obtain any seasonal constraints
        this.seasonStart = project.getEarliestDayInSeason();
        this.seasonEnd = project.getLatestDayInSeason();

        // Obtain and set the desired time scale. 
        this.desiredTimeScale = ConfigHelper.getDesiredTimeScale( pairConfig );
        
        // Create a factory for the left-ish data
        this.leftFactory = SingleValuedRetrieverFactory.of( project, feature, unitMapper );
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
            return new EnsembleForecastRetriever.Builder();
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
