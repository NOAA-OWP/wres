package wres.io.retrieval.datashop;

import java.sql.SQLException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.DoubleUnaryOperator;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.ProjectConfigException;
import wres.config.generated.DataSourceConfig;
import wres.config.generated.DatasourceType;
import wres.config.generated.DoubleBoundsType;
import wres.config.generated.Feature;
import wres.config.generated.PairConfig;
import wres.config.generated.ProjectConfig;
import wres.config.generated.ProjectConfig.Inputs;
import wres.config.generated.SourceTransformationType;
import wres.datamodel.MissingValues;
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
import wres.datamodel.time.TimeSeriesSlicer;
import wres.datamodel.time.TimeSeriesUpscaler;
import wres.datamodel.time.TimeWindow;
import wres.datamodel.time.TimeSeries.TimeSeriesBuilder;
import wres.datamodel.time.generators.PersistenceGenerator;
import wres.datamodel.time.generators.TimeWindowGenerator;
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
     * The pairer, which admits finite value pairs. TODO: expose to constructor.
     */

    private final TimeSeriesPairer<Double, Double> pairer = TimeSeriesPairerByExactTime.of( Double::isFinite,
                                                                                            Double::isFinite );

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

    public static SingleValuedPoolGenerator of( Project project, Feature feature, UnitMapper unitMapper )
    {
        return new SingleValuedPoolGenerator( project, feature, unitMapper );
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
     * @param unitMapper the unit mapper
     * @throws NullPointerException if any input is null
     * @throws IllegalArgumentException if the declaration is inconsistent with the type of pool expected
     */

    private SingleValuedPoolGenerator( Project project, Feature feature, UnitMapper unitMapper )
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

    private List<Supplier<PoolOfPairs<Double, Double>>> createPools( Project project,
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

        // Acquire any transformers needed
        DoubleUnaryOperator transformer = this.getTransformer( pairConfig.getValues() );

        // Create the common builder
        PoolOfPairsSupplierBuilder<Double, Double> builder = new PoolOfPairsSupplierBuilder<>();
        builder.setLeftUpscaler( this.getUpscaler() )
               .setRightUpscaler( this.getUpscaler() )
               .setPairer( this.getPairer() )
               .setInputsDeclaration( inputsConfig )
               .setLeftTransformer( transformer::applyAsDouble )
               .setRightTransformer( transformer::applyAsDouble );

        // Obtain and set the desired time scale. 
        TimeScale desiredTimeScale = this.setAndGetDesiredTimeScale( pairConfig, builder );

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
                        CachingRetriever.of( this.createRetriever( projectId,
                                                                   leftVariableFeatureId,
                                                                   inputsConfig.getLeft(),
                                                                   LeftOrRightOrBaseline.LEFT,
                                                                   null,
                                                                   desiredTimeScale,
                                                                   unitMapper ) );

                // Get the climatology at an appropriate scale and with any transformations required and add to the 
                // builder, but retain the existing scale for the main supplier, as that may be re-used for left data, 
                //and left data is rescaled with respect to right data
                Supplier<Stream<TimeSeries<Double>>> climatologyAtScale =
                        this.getClimatologyAtDesiredTimeScale( climatologySupplier,
                                                               this.getUpscaler(),
                                                               desiredTimeScale,
                                                               transformer );

                builder.setClimatology( climatologyAtScale, Double::doubleValue );
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

            List<Supplier<PoolOfPairs<Double, Double>>> returnMe = new ArrayList<>();

            // Create the retrievers for each time window
            for ( TimeWindow nextWindow : timeWindows )
            {
                Supplier<Stream<TimeSeries<Double>>> rightSupplier =
                        this.createRetriever( projectId,
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

                // Add left data
                // TODO: consider acquiring all the left data upfront with a caching retriever 
                // when the climatology is not available. In that case, prepare something similar to
                // climatology above, but bounded by any overall time bounds in the declaration
                Supplier<Stream<TimeSeries<Double>>> leftSupplier = climatologySupplier;

                if ( !project.usesProbabilityThresholds() )
                {
                    leftSupplier = CachingRetriever.of( this.createRetriever( projectId,
                                                                              leftVariableFeatureId,
                                                                              inputsConfig.getLeft(),
                                                                              LeftOrRightOrBaseline.LEFT,
                                                                              nextWindow,
                                                                              desiredTimeScale,
                                                                              unitMapper ) );
                }

                builder.setLeft( leftSupplier );

                // Set baseline if needed
                if ( project.hasBaseline() )
                {

                    // Set the metadata
                    SampleMetadata poolBaseMeta = SampleMetadata.of( baselineMetadata, nextWindow );
                    builder.setBaselineMetadata( poolBaseMeta );

                    // Generated baseline?
                    if ( this.hasGeneratedBaseline( projectConfig.getInputs().getBaseline() ) )
                    {
                        this.setGeneratedBaseline( projectConfig.getInputs().getBaseline(),
                                                   builder,
                                                   climatologySupplier,
                                                   this.getUpscaler(),
                                                   poolBaseMeta );
                    }
                    // Data-source baseline
                    else
                    {
                        Supplier<Stream<TimeSeries<Double>>> baselineSupplier =
                                this.createRetriever( projectId,
                                                      baselineVariableFeatureId,
                                                      inputsConfig.getBaseline(),
                                                      LeftOrRightOrBaseline.BASELINE,
                                                      nextWindow,
                                                      desiredTimeScale,
                                                      unitMapper );

                        builder.setBaseline( baselineSupplier );
                    }
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
     * Adds a generated baseline dataset to the builder if required.
     * 
     * @param baselineConfig the baseline declaration
     * @param builder the pool builder
     * @param source the data source for the generated baseline 
     * @param upscaler an upscaler, which is optional unless the generated series requires upscaling
     * @param baselineMeta the baseline metadata to assist with logging
     */

    private void setGeneratedBaseline( DataSourceConfig baselineConfig,
                                       PoolOfPairsSupplierBuilder<Double, Double> builder,
                                       Supplier<Stream<TimeSeries<Double>>> source,
                                       TimeSeriesUpscaler<Double> upscaler,
                                       SampleMetadata baselineMeta )
    {
        if ( this.hasGeneratedBaseline( baselineConfig ) )
        {
            // Persistence is supported
            if ( baselineConfig.getTransformation() == SourceTransformationType.PERSISTENCE )
            {
                LOGGER.trace( "Creating a persistence generator for pool {}.", baselineMeta );

                // Order 1 by default. If others are supported later, add these
                PersistenceGenerator<Double> generator = PersistenceGenerator.of( source, upscaler, Double::isFinite );
                builder.setBaselineGenerator( generator );
            }
            // Other types are not supported
            else
            {
                throw new UnsupportedOperationException( "While attempting to generate a baseline: unrecognized "
                                                         + "type of baseline to generate, '"
                                                         + baselineConfig.getTransformation()
                                                         + "'." );
            }
        }
    }

    /**
     * Creates a retriever.
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

    private Supplier<Stream<TimeSeries<Double>>> createRetriever( int projectId,
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
     * Sets and gets the desired time scale associated with the pair declaration.
     * 
     * TODO: consider abstracting somewhere accessible to different pool shapes, as this is pool-shape invariant. Use
     * generics to abstract.
     * 
     * @param pairConfig the pair declaration
     * @param builder the builder
     */

    private TimeScale setAndGetDesiredTimeScale( PairConfig pairConfig,
                                                 PoolOfPairsSupplierBuilder<Double, Double> builder )
    {

        TimeScale desiredTimeScale = null;
        // Obtain from the declaration if available
        if ( Objects.nonNull( pairConfig )
             && Objects.nonNull( pairConfig.getDesiredTimeScale() ) )
        {
            desiredTimeScale = TimeScale.of( pairConfig.getDesiredTimeScale() );
            builder.setDesiredTimeScale( desiredTimeScale );

            if ( Objects.nonNull( pairConfig.getDesiredTimeScale().getFrequency() ) )
            {
                ChronoUnit unit = ChronoUnit.valueOf( pairConfig.getDesiredTimeScale()
                                                                .getUnit()
                                                                .value()
                                                                .toUpperCase() );

                Duration frequency = Duration.of( pairConfig.getDesiredTimeScale().getFrequency(), unit );

                builder.setFrequencyOfPairs( frequency );
            }
        }

        return desiredTimeScale;
    }

    /**
     * Returns a climatological data supply at the desired time scale.
     * 
     * @param climatologySupplier the raw data supplier
     * @param upscaler the upscaler
     * @param desiredTimeScale the desired time scale
     * @return a climatological supply at the desired time scale
     */

    private Supplier<Stream<TimeSeries<Double>>>
            getClimatologyAtDesiredTimeScale( Supplier<Stream<TimeSeries<Double>>> climatologySupplier,
                                              TimeSeriesUpscaler<Double> upscaler,
                                              TimeScale desiredTimeScale,
                                              DoubleUnaryOperator leftTransformer )
    {
        List<TimeSeries<Double>> climData = climatologySupplier.get()
                                                               .collect( Collectors.toList() );

        TimeSeriesBuilder<Double> builder = new TimeSeriesBuilder<>();
        builder.setTimeScale( desiredTimeScale );

        for ( TimeSeries<Double> next : climData )
        {
            TimeSeries<Double> nextSeries = next;

            TimeScale nextScale = nextSeries.getTimeScale();

            // Upscale? A difference in period is the minimum needed
            if ( Objects.nonNull( desiredTimeScale )
                 && Objects.nonNull( nextScale )
                 && !desiredTimeScale.getPeriod().equals( nextScale.getPeriod() ) )
            {
                if ( Objects.isNull( upscaler ) )
                {
                    throw new IllegalArgumentException( "The climatological time-series "
                                                        + nextSeries.hashCode()
                                                        + " needed upscaling from "
                                                        + nextScale
                                                        + " to "
                                                        + desiredTimeScale
                                                        + " but no upscaler was provided.");
                }

                nextSeries = upscaler.upscale( nextSeries, desiredTimeScale )
                                     .getTimeSeries();

                LOGGER.debug( "Upscaled the climatological time-series {} from {} to {}.",
                              nextSeries.hashCode(),
                              nextScale,
                              desiredTimeScale );

            }

            // Left transformer too? Inline this to the climate mapper
            if ( Objects.nonNull( leftTransformer ) )
            {
                nextSeries = TimeSeriesSlicer.transform( nextSeries, leftTransformer::applyAsDouble );
            }

            // Filter inadmissible values. Do this LAST because a transformer may produce 
            // non-finite values
            nextSeries = TimeSeriesSlicer.filter( nextSeries, Double::isFinite );  

            builder.addEvents( nextSeries.getEvents() );
        }

        TimeSeries<Double> climatologyAtScale = builder.build();

        LOGGER.debug( "Created a new climatological time-series {} with {} climatological values.",
                      climatologyAtScale.hashCode(),
                      climatologyAtScale.getEvents().size() );

        return () -> Stream.of( climatologyAtScale );
    }

    /**
     * Returns a transformer if required.
     * 
     * @param valueConfig the value declaration 
     * @return a transformer or null
     */

    private DoubleUnaryOperator getTransformer( DoubleBoundsType valueConfig )
    {
        if ( Objects.isNull( valueConfig ) )
        {
            return DoubleUnaryOperator.identity();
        }

        double assignToLowMiss = MissingValues.DOUBLE;
        double assignToHighMiss = MissingValues.DOUBLE;

        double minimum = Double.NEGATIVE_INFINITY;
        double maximum = Double.POSITIVE_INFINITY;

        if ( Objects.nonNull( valueConfig.getDefaultMinimum() ) )
        {
            assignToLowMiss = valueConfig.getDefaultMinimum();
        }

        if ( Objects.nonNull( valueConfig.getDefaultMaximum() ) )
        {
            assignToHighMiss = valueConfig.getDefaultMaximum();
        }

        if ( Objects.nonNull( valueConfig.getMinimum() ) )
        {
            minimum = valueConfig.getMinimum();
        }

        if ( Objects.nonNull( valueConfig.getMaximum() ) )
        {
            maximum = valueConfig.getMaximum();
        }

        // Effectively final constants for use 
        // within enclosing scope
        double assignLow = assignToLowMiss;
        double assignHigh = assignToHighMiss;

        double low = minimum;
        double high = maximum;

        return toTransform -> {

            // Low miss
            if ( toTransform < low )
            {
                return assignLow;
            }

            // High miss
            if ( toTransform > high )
            {
                return assignHigh;
            }

            // Within bounds
            return toTransform;
        };
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
