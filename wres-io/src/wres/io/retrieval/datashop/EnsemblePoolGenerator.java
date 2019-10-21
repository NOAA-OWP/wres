package wres.io.retrieval.datashop;

import java.sql.SQLException;
import java.time.Duration;
import java.time.MonthDay;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.DoubleUnaryOperator;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
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
import wres.datamodel.Ensemble;
import wres.datamodel.MissingValues;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.sampledata.pairs.PoolOfPairs;
import wres.datamodel.scale.TimeScale;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesOfDoubleBasicUpscaler;
import wres.datamodel.time.TimeSeriesOfEnsembleUpscaler;
import wres.datamodel.time.TimeSeriesPairerByExactTime;
import wres.datamodel.time.TimeWindow;
import wres.datamodel.time.generators.TimeWindowGenerator;
import wres.io.config.ConfigHelper;
import wres.io.config.LeftOrRightOrBaseline;
import wres.io.project.Project;
import wres.io.retrieval.datashop.PoolOfPairsSupplier.PoolOfPairsSupplierBuilder;
import wres.io.retrieval.datashop.TimeSeriesRetriever.TimeSeriesRetrieverBuilder;

/**
 * Generates a collection of pools that contain ensemble pairs using a {@link Project} supplied on construction, 
 * together with a particular {@link Feature} for which pools are required.
 * 
 * @author james.brown@hydrosolved.com
 */

public class EnsemblePoolGenerator extends PoolGenerator<Double, Ensemble>
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
        // Set the default pairers and upscalers
        // TODO: expose as constructor arguments
        super( TimeSeriesPairerByExactTime.of( Double::isFinite,
                                               en -> Arrays.stream( en.getMembers() )
                                                           .anyMatch( Double::isFinite ) ),
               TimeSeriesOfDoubleBasicUpscaler.of(),
               TimeSeriesOfEnsembleUpscaler.of( TimeSeriesOfDoubleBasicUpscaler.of() ) );

        this.pools = this.createPools( project, feature, unitMapper );
    }

    /**
     * Produces a collection of pools from a project declaration and feature.
     * 
     * @param project the project
     * @param feature the feature
     * @param unitMapper the unit mapper
     * @return a collection of pools
     * @throws NullPointerExecption if any input is null
     * @throws PoolCreationException if the pools cannot be created for any other reason
     */

    private List<Supplier<PoolOfPairs<Double, Ensemble>>> createPools( Project project,
                                                                       Feature feature,
                                                                       UnitMapper unitMapper )
    {
        Objects.requireNonNull( project );
        Objects.requireNonNull( feature );
        Objects.requireNonNull( unitMapper );

        // Project identifier
        int projectId = project.getId();

        String featureString = ConfigHelper.getFeatureDescription( feature );

        LOGGER.debug( "Creating pool suppliers for project '{}' and feature '{}'.",
                      projectId,
                      featureString );

        ProjectConfig projectConfig = project.getProjectConfig();
        PairConfig pairConfig = projectConfig.getPair();
        Inputs inputsConfig = projectConfig.getInputs();
        DataSourceConfig leftConfig = inputsConfig.getLeft();
        DataSourceConfig rightConfig = inputsConfig.getRight();
        DataSourceConfig baselineConfig = inputsConfig.getBaseline();

        // Acquire any transformers needed
        DoubleUnaryOperator leftTransformer = this.getLeftTransformer( pairConfig.getValues() );
        UnaryOperator<Ensemble> rightTransformer = this.getRightTransformer( leftTransformer );

        // Create the common builder
        PoolOfPairsSupplierBuilder<Double, Ensemble> builder = new PoolOfPairsSupplierBuilder<>();
        builder.setLeftUpscaler( this.getLeftUpscaler() )
               .setRightUpscaler( this.getRightUpscaler() )
               .setPairer( this.getPairer() )
               .setInputsDeclaration( inputsConfig )
               .setLeftTransformer( leftTransformer::applyAsDouble )
               .setRightTransformer( rightTransformer );

        // Obtain and set the desired time scale. 
        TimeScale desiredTimeScale = this.setAndGetDesiredTimeScale( pairConfig, builder );

        // Obtain any seasonal constraints
        MonthDay seasonStart = project.getEarliestDayInSeason();
        MonthDay seasonEnd = project.getLatestDayInSeason();

        // Obtain any time offsets
        Duration leftOffset = ConfigHelper.getTimeShift( inputsConfig.getLeft() );
        Duration rightOffset = ConfigHelper.getTimeShift( inputsConfig.getRight() );
        Duration baselineOffset = ConfigHelper.getTimeShift( inputsConfig.getBaseline() );

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
                Supplier<Stream<TimeSeries<Double>>> leftSupplier =
                        this.getLeftRetrieverBuilder( leftConfig.getType() )
                            .setProjectId( projectId )
                            .setVariableFeatureId( leftVariableFeatureId )
                            .setLeftOrRightOrBaseline( LeftOrRightOrBaseline.LEFT )
                            .setDeclaredExistingTimeScale( this.getDeclaredExistingTimeScale( leftConfig ) )
                            .setDesiredTimeScale( desiredTimeScale )
                            .setUnitMapper( unitMapper )
                            .build();

                climatologySupplier = CachingRetriever.of( leftSupplier );

                // Get the climatology at an appropriate scale and with any transformations required and add to the 
                // builder, but retain the existing scale for the main supplier, as that may be re-used for left data, 
                //and left data is rescaled with respect to right data
                Supplier<Stream<TimeSeries<Double>>> climatologyAtScale =
                        this.getClimatologyAtDesiredTimeScale( climatologySupplier,
                                                               this.getLeftUpscaler(),
                                                               desiredTimeScale,
                                                               leftTransformer );

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

            List<Supplier<PoolOfPairs<Double, Ensemble>>> returnMe = new ArrayList<>();

            // Create the retrievers for each time window
            for ( TimeWindow nextWindow : timeWindows )
            {
                Supplier<Stream<TimeSeries<Ensemble>>> rightSupplier =
                        this.getRightRetrieverBuilder( rightConfig.getType() )
                            .setProjectId( projectId )
                            .setVariableFeatureId( rightVariableFeatureId )
                            .setLeftOrRightOrBaseline( LeftOrRightOrBaseline.RIGHT )
                            .setTimeWindow( nextWindow )
                            .setDeclaredExistingTimeScale( this.getDeclaredExistingTimeScale( rightConfig ) )
                            .setDesiredTimeScale( desiredTimeScale )
                            .setUnitMapper( unitMapper )
                            .setSeasonStart( seasonStart )
                            .setSeasonEnd( seasonEnd )
                            .setSeasonOffset( rightOffset )
                            .build();

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
                    leftSupplier = this.getLeftRetrieverBuilder( leftConfig.getType() )
                                       .setProjectId( projectId )
                                       .setVariableFeatureId( leftVariableFeatureId )
                                       .setLeftOrRightOrBaseline( LeftOrRightOrBaseline.LEFT )
                                       .setTimeWindow( nextWindow )
                                       .setDeclaredExistingTimeScale( this.getDeclaredExistingTimeScale( leftConfig ) )
                                       .setDesiredTimeScale( desiredTimeScale )
                                       .setUnitMapper( unitMapper )
                                       .setSeasonStart( seasonStart )
                                       .setSeasonEnd( seasonEnd )
                                       .setSeasonOffset( leftOffset )
                                       .build();
                }

                builder.setLeft( leftSupplier );

                // Set baseline if needed
                if ( project.hasBaseline() )
                {

                    // Set the metadata
                    SampleMetadata poolBaseMeta = SampleMetadata.of( baselineMetadata, nextWindow );
                    builder.setBaselineMetadata( poolBaseMeta );

                    // Data-source baseline
                    Supplier<Stream<TimeSeries<Ensemble>>> baselineSupplier =
                            this.getRightRetrieverBuilder( baselineConfig.getType() )
                                .setProjectId( projectId )
                                .setVariableFeatureId( baselineVariableFeatureId )
                                .setLeftOrRightOrBaseline( LeftOrRightOrBaseline.BASELINE )
                                .setTimeWindow( nextWindow )
                                .setDeclaredExistingTimeScale( this.getDeclaredExistingTimeScale( baselineConfig ) )
                                .setDesiredTimeScale( desiredTimeScale )
                                .setUnitMapper( unitMapper )
                                .setSeasonStart( seasonStart )
                                .setSeasonEnd( seasonEnd )
                                .setSeasonOffset( baselineOffset )
                                .build();

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
     * Returns a builder for a left-ish retriever.
     * 
     * @param dataType the retrieved data type
     * @return the retriever
     * @throws IllegalArgumentException if the data type is unrecognized in this context
     */

    private TimeSeriesRetrieverBuilder<Double> getLeftRetrieverBuilder( DatasourceType dataType )
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
     * Returns a builder for a right-ish retriever.
     * 
     * @param dataType the retrieved data type
     * @return the retriever
     * @throws IllegalArgumentException if the data type is unrecognized in this context
     */

    private TimeSeriesRetrieverBuilder<Ensemble> getRightRetrieverBuilder( DatasourceType dataType )
    {
        if ( dataType == DatasourceType.ENSEMBLE_FORECASTS )
        {
            return new EnsembleForecastRetriever.Builder();
        }
        else
        {
            throw new IllegalArgumentException( "Unrecognized data type from which to create the single-valued "
                                                + "retriever: "
                                                + dataType
                                                + "'." );
        }
    }

    /**
     * Returns a transformer for left-ish data if required.
     * 
     * @param valueConfig the value declaration 
     * @return a transformer or null
     */

    private DoubleUnaryOperator getLeftTransformer( DoubleBoundsType valueConfig )
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
     * Returns a transformer for right-ish data if required.
     * 
     * @param valueConfig the value declaration 
     * @return a transformer or null
     */

    private UnaryOperator<Ensemble> getRightTransformer( DoubleUnaryOperator leftTransformer )
    {
        if ( Objects.isNull( leftTransformer ) )
        {
            return null;
        }

        return toTransform -> {

            double[] transformed = Arrays.stream( toTransform.getMembers() )
                                         .map( leftTransformer )
                                         .toArray();

            String[] labels = null;
            if ( toTransform.getLabels().isPresent() )
            {
                labels = toTransform.getLabels().get();
            }

            return Ensemble.of( transformed, labels );
        };
    }

}
