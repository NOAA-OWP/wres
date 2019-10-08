package wres.io.retrieval.datashop;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.ProjectConfigException;
import wres.config.generated.DatasourceType;
import wres.config.generated.Feature;
import wres.config.generated.PairConfig;
import wres.config.generated.ProjectConfig;
import wres.config.generated.ProjectConfig.Inputs;
import wres.datamodel.sampledata.DatasetIdentifier;
import wres.datamodel.sampledata.Location;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.sampledata.SampleMetadata.SampleMetadataBuilder;
import wres.datamodel.sampledata.pairs.PoolOfPairs;
import wres.datamodel.scale.TimeScale;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesOfDoubleBasicUpscaler;
import wres.datamodel.time.TimeSeriesPairer;
import wres.datamodel.time.TimeSeriesPairerByExactTime;
import wres.datamodel.time.TimeSeriesUpscaler;
import wres.datamodel.time.TimeWindow;
import wres.datamodel.time.TimeWindowGenerator;
import wres.io.config.ConfigHelper;
import wres.io.config.LeftOrRightOrBaseline;
import wres.io.project.Project;
import wres.io.retrieval.datashop.PoolOfPairsSupplier.PoolOfPairsSupplierBuilder;

/**
 * Generates a collection of pools that contain single-valued pairs using a {@link Project} supplied on construction, 
 * together with a particular {@link Feature} for which pools are required.
 * 
 * @author james.brown@hydrosolved.com
 */

public class SingleValuedPoolGenerator implements Supplier<List<Supplier<PoolOfPairs<Double, Double>>>>
{

    /**
     * Logger.
     */

    private static final Logger LOGGER = LoggerFactory.getLogger( SingleValuedPoolGenerator.class );

    /**
     * The pool suppliers.
     */

    private final List<Supplier<PoolOfPairs<Double, Double>>> pools;

    /**
     * The upscaler. TODO: expose to constructor.
     */

    private final TimeSeriesUpscaler<Double> upscaler = TimeSeriesOfDoubleBasicUpscaler.of();

    /**
     * The pairer. TODO: expose to constructor.
     */

    private final TimeSeriesPairer<Double, Double> pairer = TimeSeriesPairerByExactTime.of();

    /**
     * Returns an instance that generates pools for a particular project and feature.
     * 
     * @param project the project
     * @param feature the feature 
     * @return an instance
     */

    public static SingleValuedPoolGenerator of( Project project, Feature feature )
    {
        return new SingleValuedPoolGenerator( project, feature );
    }

    @Override
    public List<Supplier<PoolOfPairs<Double, Double>>> get()
    {
        return this.pools;
    }

    /**
     * Hidden constructor.
     * 
     * @param projectthe project
     * @param feature the feature
     * @throws NullPointerException if any input is null
     * @throws IllegalArgumentException if the declaration is inconsistent with the type of pool expected
     */

    private SingleValuedPoolGenerator( Project project, Feature feature )
    {
        Objects.requireNonNull( project );
        Objects.requireNonNull( feature );

        this.pools = this.createPools( project, feature );
    }

    /**
     * Produces a collection of pools from a project declaration and feature.
     * 
     * @param project the project
     * @param feature the feature
     * @return a collection of pools
     * @throws PoolCreationException if the pools could not be created for any reason
     */

    private List<Supplier<PoolOfPairs<Double, Double>>> createPools( Project project, Feature feature )
    {
        // Project identifier
        int projectId = project.getId();

        LOGGER.debug( "Creating pool suppliers for project '{}' and feature '{}'.",
                      projectId,
                      feature );

        ProjectConfig projectConfig = project.getProjectConfig();
        PairConfig pairConfig = projectConfig.getPair();
        Inputs inputsConfig = projectConfig.getInputs();

        TimeScale desiredTimeScale = project.getDesiredTimeScale();

        // Create the common builder
        PoolOfPairsSupplierBuilder<Double, Double> builder = new PoolOfPairsSupplierBuilder<>();
        builder.setLeftUpscaler( this.getUpscaler() )
               .setRightUpscaler( this.getUpscaler() )
               .setPairer( this.getPairer() )
               .setDesiredTimeScale( desiredTimeScale );

        // Create the time windows, iterate over them and create the retrievers 
        try
        {
            // Time windows
            Set<TimeWindow> timeWindows = TimeWindowGenerator.getTimeWindowsFromPairConfig( pairConfig );

            // Data identifiers
            int leftVariableFeatureId = project.getLeftVariableFeatureId( feature );
            int rightVariableFeatureId = project.getRightVariableFeatureId( feature );

            // Unit mapper
            String desiredMeasurementUnits = pairConfig.getUnit();
            UnitMapper unitMapper = UnitMapper.of( desiredMeasurementUnits );

            // Climatological data required?
            if ( project.usesProbabilityThresholds() )
            {
                LOGGER.debug( "While genenerating pools for project '{}' and feature '{}', added a retriever for "
                              + "climatological data.",
                              projectId,
                              feature );

                // Re-use the climatology across pools with a caching retriever
                Supplier<Stream<TimeSeries<Double>>> leftSupplier =
                        CachingRetriever.of( this.createRetriever( projectId,
                                                                   leftVariableFeatureId,
                                                                   inputsConfig.getLeft().getType(),
                                                                   LeftOrRightOrBaseline.LEFT,
                                                                   null,
                                                                   desiredTimeScale,
                                                                   unitMapper ) );

                builder.setClimatology( leftSupplier, Double::doubleValue );
            }

            // Metadata
            SampleMetadata mainMetadata =
                    this.getMetadata( projectConfig,
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
                              feature );
                baselineVariableFeatureId = project.getBaselineVariableFeatureId( feature );
                baselineMetadata = this.getMetadata( projectConfig,
                                                     feature,
                                                     ConfigHelper.getVariableIdFromProjectConfig( projectConfig, true ),
                                                     inputsConfig.getBaseline().getLabel(),
                                                     desiredMeasurementUnits,
                                                     desiredTimeScale );
            }

            List<Supplier<PoolOfPairs<Double, Double>>> returnMe = new ArrayList<>();

            // Create the retrievers for each time window
            for ( TimeWindow nextWindow : timeWindows )
            {
                Supplier<Stream<TimeSeries<Double>>> rightSupplier =
                        this.createRetriever( projectId,
                                              rightVariableFeatureId,
                                              inputsConfig.getRight().getType(),
                                              LeftOrRightOrBaseline.RIGHT,
                                              nextWindow,
                                              desiredTimeScale,
                                              unitMapper );

                builder.setRight( rightSupplier );

                // Set the metadata
                SampleMetadata poolMeta = SampleMetadata.of( mainMetadata, nextWindow );
                builder.setMetadata( poolMeta );

                // Add left data if no climatology
                if ( !project.usesProbabilityThresholds() )
                {
                    // Re-use the climatology across pools with a caching retriever
                    Supplier<Stream<TimeSeries<Double>>> leftSupplier =
                            CachingRetriever.of( this.createRetriever( projectId,
                                                                       leftVariableFeatureId,
                                                                       inputsConfig.getLeft().getType(),
                                                                       LeftOrRightOrBaseline.LEFT,
                                                                       nextWindow,
                                                                       desiredTimeScale,
                                                                       unitMapper ) );
                    builder.setLeft( leftSupplier );
                }

                // Set baseline if needed
                if ( project.hasBaseline() )
                {
                    Supplier<Stream<TimeSeries<Double>>> baselineSupplier =
                            this.createRetriever( projectId,
                                                  baselineVariableFeatureId,
                                                  inputsConfig.getBaseline().getType(),
                                                  LeftOrRightOrBaseline.BASELINE,
                                                  nextWindow,
                                                  desiredTimeScale,
                                                  unitMapper );

                    builder.setBaseline( baselineSupplier );

                    // Set the metadata
                    SampleMetadata poolBaseMeta = SampleMetadata.of( baselineMetadata, nextWindow );
                    builder.setBaselineMetadata( poolBaseMeta );
                }

                returnMe.add( builder.build() );

            }

            LOGGER.debug( "Created {} pool suppliers for project '{}' and feature '{}'.",
                          returnMe.size(),
                          projectId,
                          feature );

            return Collections.unmodifiableList( returnMe );
        }
        catch ( SQLException | DataAccessException | ProjectConfigException e )
        {
            throw new PoolCreationException( "While attempting to create pools for project '" + project.getId()
                                             + "' and feature '"
                                             + feature
                                             + "':",
                                             e );
        }
    }

    /**
     * Creates a retriever.
     * 
     * @param projectId the project_id
     * @param variableFeatureId the variablefeature_id
     * @param dataType the data type
     * @param lrb the data type
     * @param timeWindow the time window
     * @param desiredTimeScale the desired time scale
     * @param unitMapper the unit mapper
     * @return the retriever
     */

    private Supplier<Stream<TimeSeries<Double>>> createRetriever( int projectId,
                                                                  int variableFeatureId,
                                                                  DatasourceType dataType,
                                                                  LeftOrRightOrBaseline lrb,
                                                                  TimeWindow timeWindow,
                                                                  TimeScale desiredTimeScale,
                                                                  UnitMapper unitMapper )
    {
        switch ( dataType )
        {
            case SINGLE_VALUED_FORECASTS:
                return new SingleValuedForecastRetriever.Builder().setProjectId( projectId )
                                                                  .setVariableFeatureId( variableFeatureId )
                                                                  .setLeftOrRightOrBaseline( lrb )
                                                                  .setTimeWindow( timeWindow )
                                                                  .setDesiredTimeScale( desiredTimeScale )
                                                                  .setUnitMapper( unitMapper )
                                                                  .build();
            case OBSERVATIONS:
            case SIMULATIONS:
                return new ObservationRetriever.Builder().setProjectId( projectId )
                                                         .setVariableFeatureId( variableFeatureId )
                                                         .setLeftOrRightOrBaseline( lrb )
                                                         .setTimeWindow( timeWindow )
                                                         .setDesiredTimeScale( desiredTimeScale )
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

    private SampleMetadata getMetadata( ProjectConfig projectConfig,
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
     * Return the upscaler.
     * 
     * @return the upscaler
     */

    private TimeSeriesUpscaler<Double> getUpscaler()
    {
        return this.upscaler;
    }

    /**
     * Return the pairer.
     * 
     * @return the pairer
     */

    private TimeSeriesPairer<Double, Double> getPairer()
    {
        return this.pairer;
    }
}
