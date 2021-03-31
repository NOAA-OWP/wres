package wres.io.pooling;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.ToDoubleFunction;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.ProjectConfigException;
import wres.config.generated.DatasourceType;
import wres.config.generated.LeftOrRightOrBaseline;
import wres.config.generated.PairConfig;
import wres.config.generated.ProjectConfig;
import wres.config.generated.ProjectConfig.Inputs;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.sampledata.pairs.PoolOfPairs;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesCrossPairer;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.datamodel.time.TimeSeriesPairer;
import wres.datamodel.time.TimeSeriesSlicer;
import wres.datamodel.time.TimeSeriesUpscaler;
import wres.datamodel.time.TimeWindowOuter;
import wres.datamodel.time.TimeSeries.TimeSeriesBuilder;
import wres.datamodel.time.generators.TimeWindowGenerator;
import wres.io.config.ConfigHelper;
import wres.io.pooling.PoolSupplier.PoolOfPairsSupplierBuilder;
import wres.io.project.Project;
import wres.io.retrieval.CachingRetriever;
import wres.io.retrieval.DataAccessException;
import wres.io.retrieval.RetrieverFactory;

/**
 * Generates a collection of {@link PoolSupplier} that contain the pools for a particular evaluation.
 * 
 * @author james.brown@hydrosolved.com
 */

public class PoolsGenerator<L, R> implements Supplier<List<Supplier<PoolOfPairs<L, R>>>>
{

    /**
     * Logger.
     */

    private static final Logger LOGGER = LoggerFactory.getLogger( PoolsGenerator.class );

    /**
     * The project for which pools are required.
     */

    private final Project project;

    /**
     * The basic metadata for the sequence of pools.
     */

    private final SampleMetadata basicMetadata;

    /**
     * The basic metadata for the sequence of pools with respect to the baseline.
     */

    private final SampleMetadata basicMetadataForBaseline;

    /**
     * A factory to create project-relevant retrievers.
     */

    private final RetrieverFactory<L, R> retrieverFactory;

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
     * An optional cross-pairer to use common pairs (by time) for the main and baseline pairs.
     */

    private final TimeSeriesCrossPairer<L, R> crossPairer;

    /**
     * A transformer that applies value constraints to left-ish values.
     */

    private final UnaryOperator<L> leftTransformer;

    /**
     * A transformer of right-ish values that can take into account the event as a whole.
     */

    private final UnaryOperator<Event<R>> rightTransformer;

    /**
     * A transformer of baseline-ish values that can take into account the event as a whole.
     */

    private final UnaryOperator<Event<R>> baselineTransformer;

    /**
     * A mapper to map between left-ish climate values and double values. TODO: propagate left-ish data for 
     * climatology, rather than transforming it upfront. 
     */

    private final ToDoubleFunction<L> climateMapper;

    /**
     * The admissible values for the left-ish data associated with climatology.
     */

    private final Predicate<L> climateAdmissibleValue;

    /**
     * An optional generator for baseline data (e.g., persistence or climatology)
     */

    private final UnaryOperator<TimeSeries<R>> baselineGenerator;

    /**
     * The pool suppliers.
     */

    private final List<Supplier<PoolOfPairs<L, R>>> pools;

    @Override
    public List<Supplier<PoolOfPairs<L, R>>> get()
    {
        return this.pools;
    }

    /**
     * A builder for the {@link PoolsGenerator}.
     */

    static class Builder<L, R>
    {

        /**
         * The project for which pools are required.
         */

        private Project project;

        /**
         * The basic metadata for the sequence of pools.
         */

        private SampleMetadata basicMetadata;

        /**
         * The basic metadata for the sequence of pools with respect to the baseline.
         */

        private SampleMetadata basicMetadataForBaseline;

        /**
         * A factory to create project-relevant retrievers.
         */

        private RetrieverFactory<L, R> retrieverFactory;

        /**
         * A function to support pairing of left and right data.
         */

        private TimeSeriesPairer<L, R> pairer;

        /**
         * An optional cross-pairer to use common pairs (by time) for the main and baseline pairs.
         */

        private TimeSeriesCrossPairer<L, R> crossPairer;

        /**
         * A function to upscale left data.
         */

        private TimeSeriesUpscaler<L> leftUpscaler;

        /**
         * A function to upscale right data.
         */

        private TimeSeriesUpscaler<R> rightUpscaler;

        /**
         * A transformer that applies value constraints to left-ish values.
         */

        private UnaryOperator<L> leftTransformer;

        /**
         * A transformer for right-ish values that can take into account the encapsulating event.
         */

        private UnaryOperator<Event<R>> rightTransformer;

        /**
         * A transformer for baseline-ish values that can take into account the encapsulating event.
         */

        private UnaryOperator<Event<R>> baselineTransformer;

        /**
         * A mapper to map between left-ish climate values and double values. TODO: make the climatology generic, 
         * specifically left-ish, throughout the software, rather than double-ish, although this carries some boxing
         * overhead for double types. See JEP 218 for a possibly better future: https://openjdk.java.net/jeps/218
         */

        private ToDoubleFunction<L> climateMapper;

        /**
         * The admissible values for the left-ish data associated with climatology.
         */

        private Predicate<L> climateAdmissibleValue;

        /**
         * An optional generator for baseline data (e.g., persistence or climatology)
         */

        private UnaryOperator<TimeSeries<R>> baselineGenerator;

        /**
         * @param project the project
         * @return the builder
         */
        Builder<L, R> setProject( Project project )
        {
            this.project = project;

            return this;
        }

        /**
         * @param basicMetadata the basic metadata for the primary pairs
         * @return the builder
         */
        Builder<L, R> setBasicMetadata( SampleMetadata basicMetadata )
        {
            this.basicMetadata = basicMetadata;

            return this;
        }

        /**
         * @param basicMetadataForBaseline the basic metadata for the baseline pairs
         * @return the builder
         */
        Builder<L, R> setBasicMetadataForBaseline( SampleMetadata basicMetadataForBaseline )
        {
            this.basicMetadataForBaseline = basicMetadataForBaseline;
            return this;
        }

        /**
         * @param retrieverFactory the retriever factory
         * @return the builder
         */
        Builder<L, R> setRetrieverFactory( RetrieverFactory<L, R> retrieverFactory )
        {
            this.retrieverFactory = retrieverFactory;

            return this;
        }

        /**
         * @param pairer the pairer
         * @return the builder
         */
        Builder<L, R> setPairer( TimeSeriesPairer<L, R> pairer )
        {
            this.pairer = pairer;

            return this;
        }

        /**
         * @param crossPairer the cross-pairer
         * @return the builder
         */
        Builder<L, R> setCrossPairer( TimeSeriesCrossPairer<L, R> crossPairer )
        {
            this.crossPairer = crossPairer;

            return this;
        }

        /**
         * @param leftUpscaler the upscaler for left values
         * @return the builder
         */
        Builder<L, R> setLeftUpscaler( TimeSeriesUpscaler<L> leftUpscaler )
        {
            this.leftUpscaler = leftUpscaler;

            return this;
        }

        /**
         * @param rightUpscaler the upscaler for right values
         * @return the builder
         */
        Builder<L, R> setRightUpscaler( TimeSeriesUpscaler<R> rightUpscaler )
        {
            this.rightUpscaler = rightUpscaler;

            return this;
        }

        /**
         * @param climatologyAdmissibleValue the admissible value constraint on climatology
         * @return the builder
         */
        Builder<L, R> setClimateAdmissibleValue( Predicate<L> climatologyAdmissibleValue )
        {
            this.climateAdmissibleValue = climatologyAdmissibleValue;

            return this;
        }

        /**
         * @param leftTransformer the left transformer
         * @return the builder
         */
        Builder<L, R> setLeftTransformer( UnaryOperator<L> leftTransformer )
        {
            this.leftTransformer = leftTransformer;

            return this;
        }

        /**
         * @param rightTransformer the right transformer, which may consider the encapsulating event
         * @return the builder
         */
        Builder<L, R> setRightTransformer( UnaryOperator<Event<R>> rightTransformer )
        {
            this.rightTransformer = rightTransformer;

            return this;
        }

        /**
         * @param baselineTransformer the baseline transformer, which may consider the encapsulating event
         * @return the builder
         */
        Builder<L, R> setBaselineTransformer( UnaryOperator<Event<R>> baselineTransformer )
        {
            this.baselineTransformer = baselineTransformer;

            return this;
        }

        /**
         * @param climateMapper the climateMapper to set
         * @return the builder
         */
        Builder<L, R> setClimateMapper( ToDoubleFunction<L> climateMapper )
        {
            this.climateMapper = climateMapper;

            return this;
        }

        /**
         * @param baselineGenerator the baselineGenerator to set
         * @return the builder
         */
        Builder<L, R> setBaselineGenerator( UnaryOperator<TimeSeries<R>> baselineGenerator )
        {
            this.baselineGenerator = baselineGenerator;

            return this;
        }

        /**
         * Builds an instance.
         * 
         * @return an instance
         */

        PoolsGenerator<L, R> build()
        {
            return new PoolsGenerator<>( this );
        }
    }

    /**
     * Hidden constructor.
     * 
     * @param builder a builder
     * @throws NullPointerException if any input is null
     * @throws IllegalArgumentException if the declaration is inconsistent with the type of pool expected
     */

    private PoolsGenerator( Builder<L, R> builder )
    {
        // Set then validate
        this.project = builder.project;
        this.retrieverFactory = builder.retrieverFactory;
        this.basicMetadata = builder.basicMetadata;
        this.basicMetadataForBaseline = builder.basicMetadataForBaseline;
        this.baselineGenerator = builder.baselineGenerator;
        this.pairer = builder.pairer;
        this.leftUpscaler = builder.leftUpscaler;
        this.rightUpscaler = builder.rightUpscaler;
        this.climateAdmissibleValue = builder.climateAdmissibleValue;
        this.leftTransformer = builder.leftTransformer;
        this.rightTransformer = builder.rightTransformer;
        this.baselineTransformer = builder.baselineTransformer;
        this.climateMapper = builder.climateMapper;
        this.crossPairer = builder.crossPairer;

        String messageStart = "Cannot build the pool generator: ";

        Objects.requireNonNull( this.project, messageStart + "the project is missing." );
        Objects.requireNonNull( this.retrieverFactory, messageStart + "the retriever factory is missing." );
        Objects.requireNonNull( this.basicMetadata, messageStart + "the basic metadata is missing." );

        Objects.requireNonNull( this.pairer, messageStart + "the pairer is missing." );
        Objects.requireNonNull( this.leftUpscaler, messageStart + "the upscaler for left values is missing" );
        Objects.requireNonNull( this.rightUpscaler, messageStart + "the upscaler for right values is missing." );

        // If adding a baseline, baseline metadata is needed. If not, it should not be supplied
        if ( this.project.hasBaseline() != Objects.nonNull( this.basicMetadataForBaseline ) )
        {
            throw new IllegalArgumentException( messageStart + "baseline metadata should be supplied when required, "
                                                + "otherwise it should not be supplied." );
        }

        // A baseline generator should be supplied if there is a baseline to generate, otherwise not
        if ( Objects.nonNull( this.baselineGenerator ) != ConfigHelper.hasGeneratedBaseline( this.project.getBaseline() ) )
        {
            throw new IllegalArgumentException( messageStart
                                                + " a baseline generator should be supplied when required, "
                                                + "otherwise it should not be supplied." );
        }

        // Create the pools
        this.pools = this.createPools();
    }

    /**
     * Produces a collection of pools.
     * 
     * @return a collection of pools
     * @throws NullPointerException if any input is null
     * @throws PoolCreationException if the pools cannot be created for any other reason
     */

    private List<Supplier<PoolOfPairs<L, R>>> createPools()
    {
        LOGGER.debug( "Creating pool suppliers for '{}'.", this.getBasicMetadata() );

        ProjectConfig projectConfig = this.getProject().getProjectConfig();
        PairConfig pairConfig = projectConfig.getPair();
        Inputs inputsConfig = projectConfig.getInputs();

        // Create the common builder
        PoolOfPairsSupplierBuilder<L, R> builder = new PoolOfPairsSupplierBuilder<>();
        builder.setLeftUpscaler( this.getLeftUpscaler() )
               .setRightUpscaler( this.getRightUpscaler() )
               .setPairer( this.getPairer() )
               .setCrossPairer( this.getCrossPairer() )
               .setInputsDeclaration( inputsConfig )
               .setLeftTransformer( this.getLeftTransformer() )
               .setRightTransformer( this.getRightTransformer() )
               .setBaselineTransformer( this.getBaselineTransformer() );

        // Obtain and set the desired time scale. 
        TimeScaleOuter desiredTimeScale = this.setAndGetDesiredTimeScale( pairConfig, builder );

        // Time windows
        Set<TimeWindowOuter> timeWindows = TimeWindowGenerator.getTimeWindowsFromPairConfig( pairConfig );

        // Get a left-ish retriever for every pool in order to promote re-use across pools via caching. May consider
        // doing this for other sides of data in future, but left-ish data is the priority because this is very 
        // often re-used
        Map<TimeWindowOuter, Supplier<Stream<TimeSeries<L>>>> leftRetrievers = this.getLeftRetrievers( timeWindows,
                                                                                                       inputsConfig.getLeft()
                                                                                                                   .getType() );

        // Create the time windows, iterate over them and create the retrievers 
        try
        {
            // Climatological data required?
            Supplier<Stream<TimeSeries<L>>> climatologySupplier = null;
            if ( this.getProject().hasProbabilityThresholds()
                 || ConfigHelper.hasGeneratedBaseline( inputsConfig.getBaseline() ) )
            {
                // Re-use the climatology across pools with a caching retriever
                Supplier<Stream<TimeSeries<L>>> leftSupplier = this.getRetrieverFactory()
                                                                   .getLeftRetriever();

                climatologySupplier = CachingRetriever.of( leftSupplier );

                // Get the climatology at an appropriate scale and with any transformations required and add to the 
                // builder, but retain the existing scale for the main supplier, as that may be re-used for left data, 
                // and left data is rescaled with respect to right data
                Supplier<Stream<TimeSeries<L>>> climatologyAtScale =
                        this.getClimatologyAtDesiredTimeScale( climatologySupplier,
                                                               this.getLeftUpscaler(),
                                                               desiredTimeScale,
                                                               this.getLeftTransformer(),
                                                               this.getClimateAdmissibleValue() );

                builder.setClimatology( climatologyAtScale, this.getClimateMapper() );
            }

            List<PoolSupplier<L, R>> returnMe = new ArrayList<>();

            // Create the retrievers for each time window
            for ( TimeWindowOuter nextWindow : timeWindows )
            {
                Supplier<Stream<TimeSeries<R>>> rightSupplier = this.getRetrieverFactory()
                                                                    .getRightRetriever( nextWindow );

                builder.setRight( rightSupplier );

                // Set the metadata
                SampleMetadata poolMeta = SampleMetadata.of( this.getBasicMetadata(), nextWindow );
                builder.setMetadata( poolMeta );

                // Add left data, using the climatology supplier first if one exists
                Supplier<Stream<TimeSeries<L>>> leftSupplier = climatologySupplier;

                if ( Objects.isNull( leftSupplier ) )
                {
                    leftSupplier = leftRetrievers.get( nextWindow );
                }

                builder.setLeft( leftSupplier );

                // Set baseline if needed
                if ( this.getProject().hasBaseline() )
                {

                    // Set the metadata
                    SampleMetadata poolBaseMeta = SampleMetadata.of( this.getBasicMetadataForBaseline(), nextWindow );
                    builder.setBaselineMetadata( poolBaseMeta );

                    // Generated baseline?
                    if ( ConfigHelper.hasGeneratedBaseline( projectConfig.getInputs().getBaseline() ) )
                    {
                        builder.setBaselineGenerator( this.getBaselineGenerator() );
                    }
                    // Data-source baseline
                    else
                    {
                        Supplier<Stream<TimeSeries<R>>> baselineSupplier = this.getRetrieverFactory()
                                                                               .getBaselineRetriever( nextWindow );

                        builder.setBaseline( baselineSupplier );
                    }
                }

                returnMe.add( builder.build() );
            }

            LOGGER.debug( "Created {} pool suppliers for '{}'.",
                          returnMe.size(),
                          this.getBasicMetadata() );

            return Collections.unmodifiableList( returnMe );
        }
        catch ( DataAccessException | ProjectConfigException e )
        {
            throw new PoolCreationException( "While attempting to create pools for '" + basicMetadata
                                             + "':",
                                             e );
        }
    }

    /**
     * Builds a left-ish retriever for each {@link TimeWindowOuter} in the input. Uses the minimum number of retrievers 
     * necessary to avoid retrieving the same data from an underlying source (e.g., database) more than once. In order,
     * to achieve this, each unique retriever is wrapped inside a {@link CachingRetriever} and re-used as many times 
     * as there are common time-windows, ignoring any lead duration bounds. The returned map has a comparator that 
     * ignores the lead duration bounds, which allows for transparent use by the caller against the original time 
     * windows. De-duplication only happens for datasets that are {@link DatasourceType#OBSERVATIONS} or 
     * {@link DatasourceType#SIMULATIONS}.
     * 
     * @param timeWindows the time windows
     * @param type the type of data
     * @return a left-ish retriever for each time-window
     */

    private Map<TimeWindowOuter, Supplier<Stream<TimeSeries<L>>>> getLeftRetrievers( Set<TimeWindowOuter> timeWindows,
                                                                                     DatasourceType type )
    {
        RetrieverFactory<L, R> factory = this.getRetrieverFactory();

        // Observations or simulations? Then de-duplicate if possible.
        if ( type == DatasourceType.OBSERVATIONS || type == DatasourceType.SIMULATIONS )
        {
            // Find the union of the time windows, bearing in mind that lead durations can influence the valid 
            // datetimes for observation selection
            TimeWindowOuter unionWindow = TimeWindowOuter.unionOf( timeWindows );
            Supplier<Stream<TimeSeries<L>>> leftRetriever = factory.getLeftRetriever( unionWindow );
            Supplier<Stream<TimeSeries<L>>> cachingRetriever = CachingRetriever.of( leftRetriever );

            // Build a retriever for each unique time window (ignoring lead durations via the comparator)
            Map<TimeWindowOuter, Supplier<Stream<TimeSeries<L>>>> returnMe = new TreeMap<>();
            timeWindows.forEach( nextWindow -> returnMe.put( nextWindow, cachingRetriever ) );

            // Log any de-duplication that was achieved
            if ( LOGGER.isDebugEnabled() )
            {
                LOGGER.debug( "While creating pools for {}, de-duplicated the retrievers of {} data from {} to {} "
                              + "using the union of time windows across all pools, which is {}.",
                              this.getBasicMetadata(),
                              LeftOrRightOrBaseline.LEFT,
                              timeWindows.size(),
                              1,
                              unionWindow );
            }

            return Collections.unmodifiableMap( returnMe );
        }
        // Other datasets: do not attempt to de-duplicate for now
        else
        {
            Map<TimeWindowOuter, Supplier<Stream<TimeSeries<L>>>> returnMe = new HashMap<>();

            timeWindows.forEach( next -> returnMe.put( next, factory.getLeftRetriever( next ) ) );

            return Collections.unmodifiableMap( returnMe );
        }
    }

    /**
     * Returns the upscaler.
     * 
     * @return the upscaler
     */

    private TimeSeriesUpscaler<L> getLeftUpscaler()
    {
        return this.leftUpscaler;
    }

    /**
     * Returns the upscaler.
     * 
     * @return the upscaler
     */

    private TimeSeriesUpscaler<R> getRightUpscaler()
    {
        return this.rightUpscaler;
    }

    /**
     * Returns the pairer.
     * 
     * @return the pairer
     */

    private TimeSeriesPairer<L, R> getPairer()
    {
        return this.pairer;
    }

    /**
     * Returns the cross pairer.
     * 
     * @return the cross pairer
     */

    private TimeSeriesCrossPairer<L, R> getCrossPairer()
    {
        return this.crossPairer;
    }

    /**
     * Returns the project.
     * 
     * @return the project
     */

    private Project getProject()
    {
        return this.project;
    }

    /**
     * Returns the transformer for left values, if any.
     * 
     * @return the transformer for left values
     */

    private UnaryOperator<L> getLeftTransformer()
    {
        return this.leftTransformer;
    }

    /**
     * Returns the transformer for right values, if any.
     * 
     * @return the transformer for right values
     */

    private UnaryOperator<Event<R>> getRightTransformer()
    {
        return this.rightTransformer;
    }

    /**
     * Returns the transformer for baseline values, if any.
     * 
     * @return the transformer for baseline values
     */

    private UnaryOperator<Event<R>> getBaselineTransformer()
    {
        return this.baselineTransformer;
    }

    /**
     * Returns the retriever factory.
     * 
     * @return the retriever factory
     */

    private RetrieverFactory<L, R> getRetrieverFactory()
    {
        return this.retrieverFactory;
    }

    /**
     * Returns the basic metadata.
     * 
     * @return the basic metadata
     */

    private SampleMetadata getBasicMetadata()
    {
        return this.basicMetadata;
    }

    /**
     * Returns the basic metadata for a baseline, if any.
     * 
     * @return the basic metadata for a baseline
     */

    private SampleMetadata getBasicMetadataForBaseline()
    {
        return this.basicMetadataForBaseline;
    }

    /**
     * Returns the baseline generator, if any.
     * 
     * @return the baseline generator
     */

    private UnaryOperator<TimeSeries<R>> getBaselineGenerator()
    {
        return this.baselineGenerator;
    }

    /**
     * Returns the admissible value guard for climatology.
     * 
     * @return the admissible value guard for climatology
     */

    private Predicate<L> getClimateAdmissibleValue()
    {
        return this.climateAdmissibleValue;
    }

    /**
     * Returns the mapper from left-ish climate values to double values.
     * 
     * @return the climate mapper
     */

    private ToDoubleFunction<L> getClimateMapper()
    {
        return this.climateMapper;
    }

    /**
     * Sets and gets the desired time scale associated with the pair declaration.
     * 
     * @param pairConfig the pair declaration
     * @param builder the builder
     */

    private TimeScaleOuter setAndGetDesiredTimeScale( PairConfig pairConfig,
                                                      PoolOfPairsSupplierBuilder<L, R> builder )
    {

        TimeScaleOuter desiredTimeScale = null;
        // Obtain from the declaration if available
        if ( Objects.nonNull( pairConfig )
             && Objects.nonNull( pairConfig.getDesiredTimeScale() ) )
        {
            desiredTimeScale = TimeScaleOuter.of( pairConfig.getDesiredTimeScale() );
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
     * @param transformer an optional transformer
     * @param admissibleValue an admissible value constraint
     * @return a climatological supply at the desired time scale
     */

    private Supplier<Stream<TimeSeries<L>>>
            getClimatologyAtDesiredTimeScale( Supplier<Stream<TimeSeries<L>>> climatologySupplier,
                                              TimeSeriesUpscaler<L> upscaler,
                                              TimeScaleOuter desiredTimeScale,
                                              UnaryOperator<L> transformer,
                                              Predicate<L> admissibleValue )
    {
        List<TimeSeries<L>> climData = climatologySupplier.get()
                                                          .collect( Collectors.toList() );

        TimeSeriesBuilder<L> builder = new TimeSeriesBuilder<>();

        TimeSeriesMetadata existingMetadata = null;

        for ( TimeSeries<L> next : climData )
        {
            TimeSeries<L> nextSeries = next;
            TimeScaleOuter nextScale = nextSeries.getMetadata()
                                                 .getTimeScale();

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

            // Transform?
            if ( Objects.nonNull( transformer ) )
            {
                nextSeries = TimeSeriesSlicer.transform( nextSeries, transformer );
            }

            // Filter inadmissible values. Do this LAST because a transformer may produce 
            // non-finite values
            nextSeries = TimeSeriesSlicer.filter( nextSeries, admissibleValue );

            existingMetadata = nextSeries.getMetadata();
            builder.addEvents( nextSeries.getEvents() );
        }

        TimeSeriesMetadata metadata =
                new TimeSeriesMetadata.Builder( existingMetadata ).setTimeScale( desiredTimeScale )
                                                                  .build();
        builder.setMetadata( metadata );

        TimeSeries<L> climatologyAtScale = builder.build();

        LOGGER.debug( "Created a new climatological time-series {} with {} climatological values.",
                      climatologyAtScale.hashCode(),
                      climatologyAtScale.getEvents().size() );

        return () -> Stream.of( climatologyAtScale );
    }

}
