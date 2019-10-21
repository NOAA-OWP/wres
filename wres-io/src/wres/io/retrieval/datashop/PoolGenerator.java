package wres.io.retrieval.datashop;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Objects;
import java.util.function.DoubleUnaryOperator;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.Feature;
import wres.config.generated.PairConfig;
import wres.config.generated.ProjectConfig;
import wres.config.generated.SourceTransformationType;
import wres.datamodel.sampledata.DatasetIdentifier;
import wres.datamodel.sampledata.Location;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.sampledata.SampleMetadata.SampleMetadataBuilder;
import wres.datamodel.sampledata.pairs.PoolOfPairs;
import wres.datamodel.scale.TimeScale;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesPairer;
import wres.datamodel.time.TimeSeriesSlicer;
import wres.datamodel.time.TimeSeriesUpscaler;
import wres.datamodel.time.TimeSeries.TimeSeriesBuilder;
import wres.datamodel.time.generators.PersistenceGenerator;
import wres.io.retrieval.datashop.PoolOfPairsSupplier.PoolOfPairsSupplierBuilder;

/**
 * An abstract base class for the generation of a sequence of pools that contains paired values.
 * 
 * @author james.brown@hydrosolved.com
 * @param <L> the left type of paired data
 * @param <R> the right type of paired data 
 */

abstract class PoolGenerator<L, R> implements Supplier<List<Supplier<PoolOfPairs<L, R>>>>
{

    /**
     * Logger.
     */

    private static final Logger LOGGER = LoggerFactory.getLogger( PoolGenerator.class );

    /**
     * The upscaler for left-ish values.
     */

    private final TimeSeriesUpscaler<L> leftUpscaler;

    /**
     * The upscaler for right-ish values.
     */

    private final TimeSeriesUpscaler<R> rightUpscaler;

    /**
     * The pairer, which admits finite value pairs.
     */

    private final TimeSeriesPairer<L, R> pairer;


    /**
     * Returns <code>true</code> if a generated baseline is required, otherwise <code>false</code>.
     * 
     * @param baselineConfig the declaration to inspect
     * @return true if a generated baseline is required
     */

    boolean hasGeneratedBaseline( DataSourceConfig baselineConfig )
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

    void setGeneratedBaseline( DataSourceConfig baselineConfig,
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

    SampleMetadata createMetadata( ProjectConfig projectConfig,
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

    TimeScale setAndGetDesiredTimeScale( PairConfig pairConfig,
                                         PoolOfPairsSupplierBuilder<L, R> builder )
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

    Supplier<Stream<TimeSeries<Double>>>
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
                                                        + " but no upscaler was provided." );
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
     * Returns the declared existing time scale associated with a data source, if any.
     * 
     * @param dataSourceConfig the data source declaration
     * @return a declared existing time scale, or null
     */

    TimeScale getDeclaredExistingTimeScale( DataSourceConfig dataSourceConfig )
    {
        // Declared existing scale, which can be used to augment a source
        TimeScale declaredExistingTimeScale = null;

        if ( Objects.nonNull( dataSourceConfig.getExistingTimeScale() ) )
        {
            declaredExistingTimeScale = TimeScale.of( dataSourceConfig.getExistingTimeScale() );
        }

        return declaredExistingTimeScale;
    }
    
    /**
     * Return the upscaler.
     * 
     * @return the upscaler
     */

    TimeSeriesUpscaler<L> getLeftUpscaler()
    {
        return this.leftUpscaler;
    }

    /**
     * Return the upscaler.
     * 
     * @return the upscaler
     */

    TimeSeriesUpscaler<R> getRightUpscaler()
    {
        return this.rightUpscaler;
    }

    /**
     * Return the pairer.
     * 
     * @return the pairer
     */

    TimeSeriesPairer<L, R> getPairer()
    {
        return this.pairer;
    }

    /**
     * Constructor.
     * 
     * @param pairer the pairer
     * @param leftUpscaler the left upscaler
     * @param rightUpscaler the right upscaler 
     */

    PoolGenerator( TimeSeriesPairer<L, R> pairer,
                   TimeSeriesUpscaler<L> leftUpscaler,
                   TimeSeriesUpscaler<R> rightUpscaler )
    {
        Objects.requireNonNull( pairer );
        Objects.requireNonNull( leftUpscaler );
        Objects.requireNonNull( rightUpscaler );

        this.leftUpscaler = leftUpscaler;
        this.rightUpscaler = rightUpscaler;
        this.pairer = pairer;
    }

}
