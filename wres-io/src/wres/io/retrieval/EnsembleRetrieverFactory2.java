package wres.io.retrieval;

import java.util.Objects;
import java.util.Set;
import java.util.function.DoubleUnaryOperator;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.LeftOrRightOrBaseline;
import wres.config.generated.PairConfig;
import wres.config.generated.ProjectConfig;
import wres.datamodel.Ensemble;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.FeatureKey;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.datamodel.time.TimeSeriesSlicer;
import wres.datamodel.time.TimeSeriesStore;
import wres.datamodel.time.TimeWindowOuter;
import wres.io.config.ConfigHelper;

/**
 * <p>A factory class that creates retrievers for the single-valued left and ensemble right datasets associated with one 
 * evaluation. This factory takes a "per-feature-view" of retrieval whereby a feature is supplied on construction. In 
 * future, other implementations may not take a per-feature view (e.g., a multiple-feature-view or a grid-view). This 
 * implementation is backed by a {@link TimeSeriesStore} that is supplied on construction.
 * 
 * @author james.brown@hydrosolved.com
 */

public class EnsembleRetrieverFactory2 implements RetrieverFactory<Double, Ensemble>
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( EnsembleRetrieverFactory2.class );

    /** Declared <code>desiredTimeScale</code>, if any. */
    private final TimeScaleOuter desiredTimeScale;

    /** A time-series store. */
    private final TimeSeriesStore timeSeriesStore;

    /**A unit mapper. */
    private final UnitMapper unitMapper;

    /**
     * Returns an instance.
     *
     * @param projectConfig the project declaration
     * @param timeSeriesStore the store of time-series
     * @param unitMapper the unit mapper
     * @return a factory instance
     * @throws NullPointerException if any input is null
     */

    public static EnsembleRetrieverFactory2 of( ProjectConfig projectConfig,
                                                TimeSeriesStore timeSeriesStore,
                                                UnitMapper unitMapper )
    {
        return new EnsembleRetrieverFactory2( projectConfig, timeSeriesStore, unitMapper );
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
        // Wrap in a caching retriever to allow re-use of left-ish data, and map the units here
        return CachingRetriever.of( () -> this.timeSeriesStore.getSingleValuedSeries( LeftOrRightOrBaseline.LEFT,
                                                                                      features.iterator().next() )
                                                              .map( this::mapUnits ) );
    }

    @Override
    public Supplier<Stream<TimeSeries<Double>>> getLeftRetriever( Set<FeatureKey> features,
                                                                  TimeWindowOuter timeWindow )
    {
        // Consider all possible lead durations
        TimeWindowOuter adjustedWindow = timeWindow.toBuilder()
                                                   .setEarliestLeadDuration( TimeWindowOuter.DURATION_MIN )
                                                   .setLatestLeadDuration( TimeWindowOuter.DURATION_MAX )
                                                   .build();

        // Wrap in a caching retriever to allow re-use of left-ish data
        return CachingRetriever.of( () -> this.timeSeriesStore.getSingleValuedSeries( adjustedWindow,
                                                                                      LeftOrRightOrBaseline.LEFT,
                                                                                      features.iterator().next() )
                                                              .map( this::mapUnits ) );
    }

    @Override
    public Supplier<Stream<TimeSeries<Ensemble>>> getRightRetriever( Set<FeatureKey> features,
                                                                     TimeWindowOuter timeWindow )
    {
        TimeWindowOuter adjustedWindow = this.adjustByTimeScalePeriod( timeWindow );

        // TODO: allow for unit mapping
        return () -> this.timeSeriesStore.getEnsembleSeries( adjustedWindow,
                                                             LeftOrRightOrBaseline.RIGHT,
                                                             features.iterator().next() );
    }

    @Override
    public Supplier<Stream<TimeSeries<Ensemble>>> getBaselineRetriever( Set<FeatureKey> features )
    {
        // TODO: allow for unit mapping
        return this.getBaselineRetriever( features, TimeWindowOuter.of() );
    }

    @Override
    public Supplier<Stream<TimeSeries<Ensemble>>> getBaselineRetriever( Set<FeatureKey> features,
                                                                        TimeWindowOuter timeWindow )
    {
        TimeWindowOuter adjustedWindow = this.adjustByTimeScalePeriod( timeWindow );

        // TODO: allow for unit mapping
        return () -> this.timeSeriesStore.getEnsembleSeries( adjustedWindow,
                                                             LeftOrRightOrBaseline.BASELINE,
                                                             features.iterator().next() );
    }

    /**
     * Adjusts the earliest lead duration of the input to account for the period associated with the desired time scale 
     * in order to capture sufficient data for rescaling.
     * 
     * @param timeWindow the time window to adjust
     * @return the adjusted time window
     */

    private TimeWindowOuter adjustByTimeScalePeriod( TimeWindowOuter timeWindow )
    {
        TimeWindowOuter adjusted = timeWindow.toBuilder()
                                             .setEarliestLeadDuration( timeWindow.getEarliestLeadDuration()
                                                                                 .minus( this.desiredTimeScale.getPeriod() ) )
                                             .build();

        if ( LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( "Adjusted the earliest lead duration of {} by {} to {}, in order to select sufficient data "
                          + "for rescaling.",
                          timeWindow,
                          this.desiredTimeScale.getPeriod(),
                          adjusted );
        }

        return adjusted;
    }

    /**
     * Map the units of a single-valued time-series
     * @param timeSeries the time-series
     * @return a time-series with mapped units
     */

    private final TimeSeries<Double> mapUnits( TimeSeries<Double> timeSeries )
    {
        DoubleUnaryOperator mapper = this.unitMapper.getUnitMapper( timeSeries.getMetadata().getUnit() );

        TimeSeriesMetadata meta =
                new TimeSeriesMetadata.Builder( timeSeries.getMetadata() ).setUnit( this.unitMapper.getDesiredMeasurementUnitName() )
                                                                          .build();
        TimeSeries<Double> mapped = TimeSeriesSlicer.transform( timeSeries, mapper::applyAsDouble );
        return TimeSeries.of( meta, mapped.getEvents() );
    }

    /**
     * Hidden constructor.
     * 
     * @param projectConfig the project declaration
     * @param timeSeriesStore the time-series store
     * @param unitMapper the unit mapper
     * @param timeSeriesStore the store of time-series
     * @throws NullPointerException if any input is null
     */

    private EnsembleRetrieverFactory2( ProjectConfig projectConfig,
                                       TimeSeriesStore timeSeriesStore,
                                       UnitMapper unitMapper )
    {
        Objects.requireNonNull( projectConfig );
        Objects.requireNonNull( timeSeriesStore );
        Objects.requireNonNull( unitMapper );

        this.timeSeriesStore = timeSeriesStore;
        this.unitMapper = unitMapper;

        PairConfig pairConfig = projectConfig.getPair();

        // Obtain and set the desired time scale. 
        this.desiredTimeScale = ConfigHelper.getDesiredTimeScale( pairConfig );
    }

}
