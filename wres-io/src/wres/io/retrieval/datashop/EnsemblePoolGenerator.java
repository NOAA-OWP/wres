package wres.io.retrieval.datashop;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.ProjectConfigException;
import wres.config.generated.DataSourceConfig;
import wres.config.generated.DatasourceType;
import wres.config.generated.Feature;
import wres.config.generated.PairConfig;
import wres.config.generated.ProjectConfig;
import wres.config.generated.ProjectConfig.Inputs;
import wres.config.generated.SourceTransformationType;
import wres.datamodel.Ensemble;
import wres.datamodel.sampledata.DatasetIdentifier;
import wres.datamodel.sampledata.Location;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.sampledata.SampleMetadata.SampleMetadataBuilder;
import wres.datamodel.sampledata.pairs.PoolOfPairs;
import wres.datamodel.scale.TimeScale;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesOfDoubleBasicUpscaler;
import wres.datamodel.time.TimeSeriesOfEnsembleUpscaler;
import wres.datamodel.time.TimeSeriesPairer;
import wres.datamodel.time.TimeSeriesPairerByExactTime;
import wres.datamodel.time.TimeSeriesUpscaler;
import wres.datamodel.time.TimeWindow;
import wres.datamodel.time.generators.TimeWindowGenerator;
import wres.io.config.ConfigHelper;
import wres.io.config.LeftOrRightOrBaseline;
import wres.io.project.Project;
import wres.io.retrieval.datashop.PoolOfPairsSupplier.PoolOfPairsSupplierBuilder;

/**
 * Generates a collection of pools that contain ensemble pairs using a {@link Project} supplied on construction, 
 * together with a particular {@link Feature} for which pools are required.
 * 
 * @author james.brown@hydrosolved.com
 */

public class EnsemblePoolGenerator implements Supplier<List<Supplier<PoolOfPairs<Double, Ensemble>>>>
{

    /**
     * Logger.
     */

    private static final Logger LOGGER = LoggerFactory.getLogger( EnsemblePoolGenerator.class );

    /**
     * The pool suppliers.
     */

    private final List<Supplier<PoolOfPairs<Double, Ensemble>>> pools;

    /**
     * The left upscaler. TODO: expose to constructor.
     */

    private final TimeSeriesUpscaler<Double> leftUpscaler = TimeSeriesOfDoubleBasicUpscaler.of();

    /**
     * The right upscaler, which uses the same strategy as the left. TODO: expose to constructor.
     */

    private final TimeSeriesUpscaler<Ensemble> rightUpscaler = TimeSeriesOfEnsembleUpscaler.of( this.leftUpscaler );

    /**
     * The pairer, which admits finite value pairs. TODO: expose to constructor.
     */

    private final TimeSeriesPairer<Double, Ensemble> pairer =
            TimeSeriesPairerByExactTime.of( Double::isFinite,
                                            en -> Arrays.stream( en.getMembers() )
                                                        .allMatch( Double::isFinite ) );

    /**
     * Returns an instance that generates pools for a particular project and feature.
     * 
     * @param project the project
     * @param feature the feature 
     * @param unitMapper the unit mapper
     * @return an instance
     * @throws NullPointerException if any input is null
     * @throws IllegalArgumentException if the declaration is inconsistent with the type of pool expected 
     */

    public static EnsemblePoolGenerator of( Project project, Feature feature, UnitMapper unitMapper )
    {
        return new EnsemblePoolGenerator( project, feature, unitMapper );
    }

    @Override
    public List<Supplier<PoolOfPairs<Double, Ensemble>>> get()
    {
        return this.pools;
    }

    /**
     * Hidden constructor.
     * 
     * @param projectthe project
     * @param feature the feature
     * @param unitMapper the unit mapper
     * @throws NullPointerException if any input is null
     * @throws IllegalArgumentException if the declaration is inconsistent with the type of pool expected
     */

    private EnsemblePoolGenerator( Project project, Feature feature, UnitMapper unitMapper )
    {
        Objects.requireNonNull( project );
        Objects.requireNonNull( feature );
        Objects.requireNonNull( unitMapper );

        this.pools = this.createPools( project, feature, unitMapper );
    }

    /**
     * Produces a collection of pools from a project declaration and feature.
     * 
     * @param project the project
     * @param feature the feature
     * @param unitMapper the unit mapper
     * @return a collection of pools
     * @throws PoolCreationException if the pools could not be created for any reason
     */

    private List<Supplier<PoolOfPairs<Double, Ensemble>>> createPools( Project project,
                                                                       Feature feature,
                                                                       UnitMapper unitMapper )
    {
        // Project identifier
        int projectId = project.getId();

        String featureString = ConfigHelper.getFeatureDescription( feature );

        LOGGER.debug( "Creating pool suppliers for project '{}' and feature '{}'.",
                      projectId,
                      featureString );

        ProjectConfig projectConfig = project.getProjectConfig();
        PairConfig pairConfig = projectConfig.getPair();
        Inputs inputsConfig = projectConfig.getInputs();

        // Create the common builder
        PoolOfPairsSupplierBuilder<Double, Ensemble> builder = new PoolOfPairsSupplierBuilder<>();
        builder.setLeftUpscaler( this.getLeftUpscaler() )
               .setRightUpscaler( this.getRightUpscaler() )
               .setPairer( this.getPairer() )
               .setInputsDeclaration( inputsConfig );

        // Obtain the desired time scale. 
        TimeScale desiredTimeScale = null;

        // Obtain from the declaration if available
        if ( Objects.nonNull( pairConfig )
             && Objects.nonNull( pairConfig.getDesiredTimeScale() ) )
        {
            desiredTimeScale = TimeScale.of( projectConfig.getPair().getDesiredTimeScale() );
            builder.setDesiredTimeScale( desiredTimeScale );
        }

        // Create the time windows, iterate over them and create the retrievers 
        try
        {
            // Time windows
            Set<TimeWindow> timeWindows = TimeWindowGenerator.getTimeWindowsFromPairConfig( pairConfig );

            // Data identifiers
            int leftVariableFeatureId = project.getLeftVariableFeatureId( feature );
            int rightVariableFeatureId = project.getRightVariableFeatureId( feature );

            String desiredMeasurementUnits = pairConfig.getUnit();

            // Climatological data required?
            Supplier<Stream<TimeSeries<Double>>> climatologySupplier = null;
            if ( project.usesProbabilityThresholds() || this.hasGeneratedBaseline( inputsConfig.getBaseline() ) )
            {
                LOGGER.debug( "While genenerating pools for project '{}' and feature '{}', added a retriever for "
                              + "climatological data.",
                              projectId,
                              featureString );

                // Re-use the climatology across pools with a caching retriever
                climatologySupplier =
                        CachingRetriever.of( this.createLeftRetriever( projectId,
                                                                       leftVariableFeatureId,
                                                                       inputsConfig.getLeft(),
                                                                       LeftOrRightOrBaseline.LEFT,
                                                                       null,
                                                                       desiredTimeScale,
                                                                       unitMapper ) );

                builder.setClimatology( climatologySupplier, Double::doubleValue );
            }

            // Metadata
            SampleMetadata mainMetadata =
                    this.createMetadata( projectConfig,
                                         feature,
                                         ConfigHelper.getVariableIdFromProjectConfig( projectConfig, false ),
                                         inputsConfig.getRight().getLabel(),
                                         desiredMeasurementUnits,
                                         desiredTimeScale );

            SampleMetadata baselineMetadata = null;

            // Baseline?
            Integer baselineVariableFeatureId = null; // Possibly null
            if ( project.hasBaseline() )
            {
                LOGGER.debug( "While genenerating pools for project '{}' and feature '{}', discovered a baseline data "
                              + "source to retrieve.",
                              projectId,
                              featureString );
                baselineVariableFeatureId = project.getBaselineVariableFeatureId( feature );
                baselineMetadata = this.createMetadata( projectConfig,
                                                        feature,
                                                        ConfigHelper.getVariableIdFromProjectConfig( projectConfig,
                                                                                                     true ),
                                                        inputsConfig.getBaseline().getLabel(),
                                                        desiredMeasurementUnits,
                                                        desiredTimeScale );
            }

            List<Supplier<PoolOfPairs<Double, Ensemble>>> returnMe = new ArrayList<>();

            // Create the retrievers for each time window
            for ( TimeWindow nextWindow : timeWindows )
            {
                Supplier<Stream<TimeSeries<Ensemble>>> rightSupplier =
                        this.createRightRetriever( projectId,
                                                   rightVariableFeatureId,
                                                   inputsConfig.getRight(),
                                                   LeftOrRightOrBaseline.RIGHT,
                                                   nextWindow,
                                                   desiredTimeScale,
                                                   unitMapper );

                builder.setRight( rightSupplier );

                // Set the metadata
                SampleMetadata poolMeta = SampleMetadata.of( mainMetadata, nextWindow );
                builder.setMetadata( poolMeta );

                // Add left data if no climatology
                // TODO: consider acquiring all the left data upfront with a caching retriever 
                // when the climatology is not available. In that case, prepare something similar to
                // climatology above, but bounded by any overall time bounds in the declaration
                if ( !project.usesProbabilityThresholds() )
                {
                    // Re-use the climatology across pools with a caching retriever
                    Supplier<Stream<TimeSeries<Double>>> leftSupplier =
                            CachingRetriever.of( this.createLeftRetriever( projectId,
                                                                           leftVariableFeatureId,
                                                                           inputsConfig.getLeft(),
                                                                           LeftOrRightOrBaseline.LEFT,
                                                                           nextWindow,
                                                                           desiredTimeScale,
                                                                           unitMapper ) );
                    builder.setLeft( leftSupplier );
                }

                // Set baseline if needed
                if ( project.hasBaseline() )
                {

                    // Set the metadata
                    SampleMetadata poolBaseMeta = SampleMetadata.of( baselineMetadata, nextWindow );
                    builder.setBaselineMetadata( poolBaseMeta );

                    // Data-source baseline
                    Supplier<Stream<TimeSeries<Ensemble>>> baselineSupplier =
                            this.createRightRetriever( projectId,
                                                       baselineVariableFeatureId,
                                                       inputsConfig.getBaseline(),
                                                       LeftOrRightOrBaseline.BASELINE,
                                                       nextWindow,
                                                       desiredTimeScale,
                                                       unitMapper );

                    builder.setBaseline( baselineSupplier );
                }

                returnMe.add( builder.build() );

            }

            LOGGER.debug( "Created {} pool suppliers for project '{}' and feature '{}'.",
                          returnMe.size(),
                          projectId,
                          featureString );

            return Collections.unmodifiableList( returnMe );
        }
        catch ( SQLException | DataAccessException | ProjectConfigException e )
        {
            throw new PoolCreationException( "While attempting to create pools for project '" + project.getId()
                                             + "' and feature '"
                                             + featureString
                                             + "':",
                                             e );
        }
    }

    /**
     * Returns <code>true</code> if a generated baseline is required, otherwise <code>false</code>.
     * 
     * @param baselineConfig the declaration to inspect
     * @return true if a generated baseline is required
     */

    private boolean hasGeneratedBaseline( DataSourceConfig baselineConfig )
    {
        // Currently only one generated type supported
        return Objects.nonNull( baselineConfig )
               && baselineConfig.getTransformation() == SourceTransformationType.PERSISTENCE;
    }

    /**
     * Creates a retriever for left data.
     * 
     * @param projectId the project_id
     * @param variableFeatureId the variablefeature_id
     * @param dataSource the data sourece declaration
     * @param lrb the data type
     * @param timeWindow the time window
     * @param desiredTimeScale the desired time scale
     * @param unitMapper the unit mapper
     * @return the retriever
     */

    private Supplier<Stream<TimeSeries<Double>>> createLeftRetriever( int projectId,
                                                                      int variableFeatureId,
                                                                      DataSourceConfig dataSource,
                                                                      LeftOrRightOrBaseline lrb,
                                                                      TimeWindow timeWindow,
                                                                      TimeScale desiredTimeScale,
                                                                      UnitMapper unitMapper )
    {

        // Type to iterate
        DatasourceType dataType = dataSource.getType();

        // Declared existing scale, which can be used to augment a source
        TimeScale declaredExistingTimeScale = null;

        if ( Objects.nonNull( dataSource.getExistingTimeScale() ) )
        {
            declaredExistingTimeScale = TimeScale.of( dataSource.getExistingTimeScale() );
        }

        switch ( dataType )
        {
            case SINGLE_VALUED_FORECASTS:
                return new SingleValuedForecastRetriever.Builder().setProjectId( projectId )
                                                                  .setVariableFeatureId( variableFeatureId )
                                                                  .setLeftOrRightOrBaseline( lrb )
                                                                  .setTimeWindow( timeWindow )
                                                                  .setDesiredTimeScale( desiredTimeScale )
                                                                  .setDeclaredExistingTimeScale( declaredExistingTimeScale )
                                                                  .setUnitMapper( unitMapper )
                                                                  .build();
            case OBSERVATIONS:
            case SIMULATIONS:
                return new ObservationRetriever.Builder().setProjectId( projectId )
                                                         .setVariableFeatureId( variableFeatureId )
                                                         .setLeftOrRightOrBaseline( lrb )
                                                         .setTimeWindow( timeWindow )
                                                         .setDesiredTimeScale( desiredTimeScale )
                                                         .setDeclaredExistingTimeScale( declaredExistingTimeScale )
                                                         .setUnitMapper( unitMapper )
                                                         .build();
            default:
                throw new IllegalArgumentException( "Unrecognized data type from which to create the single-valued "
                                                    + "retriever: "
                                                    + dataType
                                                    + "'." );
        }
    }

    /**
     * Creates a retriever for left data.
     * 
     * @param projectId the project_id
     * @param variableFeatureId the variablefeature_id
     * @param dataSource the data sourece declaration
     * @param lrb the data type
     * @param timeWindow the time window
     * @param desiredTimeScale the desired time scale
     * @param unitMapper the unit mapper
     * @return the retriever
     */

    private Supplier<Stream<TimeSeries<Ensemble>>> createRightRetriever( int projectId,
                                                                         int variableFeatureId,
                                                                         DataSourceConfig dataSource,
                                                                         LeftOrRightOrBaseline lrb,
                                                                         TimeWindow timeWindow,
                                                                         TimeScale desiredTimeScale,
                                                                         UnitMapper unitMapper )
    {

        // Type to iterate
        DatasourceType dataType = dataSource.getType();

        // Declared existing scale, which can be used to augment a source
        TimeScale declaredExistingTimeScale = null;

        if ( Objects.nonNull( dataSource.getExistingTimeScale() ) )
        {
            declaredExistingTimeScale = TimeScale.of( dataSource.getExistingTimeScale() );
        }

        if ( dataType == DatasourceType.ENSEMBLE_FORECASTS )
        {
            return new EnsembleForecastRetriever.Builder().setProjectId( projectId )
                                                          .setVariableFeatureId( variableFeatureId )
                                                          .setLeftOrRightOrBaseline( lrb )
                                                          .setTimeWindow( timeWindow )
                                                          .setDesiredTimeScale( desiredTimeScale )
                                                          .setDeclaredExistingTimeScale( declaredExistingTimeScale )
                                                          .setUnitMapper( unitMapper )
                                                          .build();
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
     * Returns a metadata representation of the input.
     * 
     * @param projectConfig the project declaration
     * @param feature the feature
     * @param variableId the variable identifier
     * @param scenarioId the scenario identifier
     * @param measurementUnitString the measurement units string
     * @param desiredTimeScale the desired time scale
     * @return the metadata
     */

    private SampleMetadata createMetadata( ProjectConfig projectConfig,
                                           Feature feature,
                                           String variableId,
                                           String scenarioId,
                                           String measurementUnitString,
                                           TimeScale desiredTimeScale )
    {
        Float longitude = null;
        Float latitude = null;

        if ( Objects.nonNull( feature.getCoordinate() ) )
        {
            longitude = feature.getCoordinate().getLongitude();
            latitude = feature.getCoordinate().getLatitude();
        }

        Location location = Location.of( feature.getComid(),
                                         feature.getLocationId(),
                                         longitude,
                                         latitude,
                                         feature.getGageId() );


        DatasetIdentifier identifier = DatasetIdentifier.of( location, variableId, scenarioId );

        MeasurementUnit measurementUnit = MeasurementUnit.of( measurementUnitString );

        return new SampleMetadataBuilder().setIdentifier( identifier )
                                          .setProjectConfig( projectConfig )
                                          .setMeasurementUnit( measurementUnit )
                                          .setTimeScale( desiredTimeScale )
                                          .build();
    }

    /**
     * Return the upscaler for left values.
     * 
     * @return the upscaler for left values
     */

    private TimeSeriesUpscaler<Double> getLeftUpscaler()
    {
        return this.leftUpscaler;
    }

    /**
     * Return the upscaler for right values.
     * 
     * @return the upscaler for right values
     */

    private TimeSeriesUpscaler<Ensemble> getRightUpscaler()
    {
        return this.rightUpscaler;
    }

    /**
     * Return the pairer.
     * 
     * @return the pairer
     */

    private TimeSeriesPairer<Double, Ensemble> getPairer()
    {
        return this.pairer;
    }
}
