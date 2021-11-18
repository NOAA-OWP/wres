package wres.datamodel.pools;

import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import wres.datamodel.messages.MessageFactory;
import wres.datamodel.messages.MessageUtilities;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.FeatureGroup;
import wres.datamodel.space.FeatureTuple;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.datamodel.time.TimeWindowOuter;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.Pool;

/**
 * An immutable store of metadata that describes a {@link Pool}.
 * 
 * @author James Brown
 */
public class PoolMetadata implements Comparable<PoolMetadata>
{

    /**
     * A description of the evaluation.
     */

    private final Evaluation evaluation;

    /**
     * A description of the pool to which the sample data belongs.
     */

    private final Pool pool;

    /**
     * Creates an instance from an {@link Evaluation} and a {@link Pool}.
     * 
     * @param evaluation the evaluation
     * @param pool the pool
     * @return an instance
     * @throws NullPointerException if either input is null.
     */

    public static PoolMetadata of( Evaluation evaluation, Pool pool )
    {
        return new PoolMetadata( evaluation, pool );
    }

    /**
     * Build a {@link PoolMetadata} object with a default {@link MeasurementUnit} of 
     * {@link MeasurementUnit#DIMENSIONLESS} and a default pool, {@link Pool#getDefaultInstance()}.
     * 
     * @return a {@link PoolMetadata} object
     */

    public static PoolMetadata of()
    {
        Evaluation evaluation = Evaluation.newBuilder()
                                          .setMeasurementUnit( MeasurementUnit.DIMENSIONLESS )
                                          .build();

        Pool pool = Pool.getDefaultInstance();

        return new PoolMetadata( evaluation, pool );
    }

    /**
     * Build a {@link PoolMetadata} object with a default {@link MeasurementUnit} of 
     * {@link MeasurementUnit#DIMENSIONLESS} and the status of the pool as a regular pool or baseline pool.
     * 
     * @param isBaselinePool is true if the pool is a baseline pool, otherwise false
     * @return a {@link PoolMetadata} object
     */

    public static PoolMetadata of( boolean isBaselinePool )
    {
        Evaluation evaluation = Evaluation.newBuilder()
                                          .setMeasurementUnit( MeasurementUnit.DIMENSIONLESS )
                                          .build();

        Pool pool = Pool.newBuilder()
                        .setIsBaselinePool( isBaselinePool )
                        .build();

        return new PoolMetadata( evaluation, pool );
    }

    /**
     * Builds a {@link PoolMetadata} from a prescribed input source and an override {@link OneOrTwoThresholds}.
     * 
     * @param input the source metadata
     * @param thresholds the thresholds
     * @return a {@link PoolMetadata} object
     * @throws NullPointerException if any required input is null
     */

    public static PoolMetadata of( PoolMetadata input, OneOrTwoThresholds thresholds )
    {
        Objects.requireNonNull( input );

        Evaluation evaluation = input.getEvaluation();

        Pool.Builder pool = input.getPool().toBuilder();

        if ( Objects.nonNull( thresholds ) )
        {
            pool.setEventThreshold( thresholds.first().getThreshold() );
            if ( thresholds.hasTwo() )
            {
                pool.setDecisionThreshold( thresholds.second().getThreshold() );
            }
        }

        return new PoolMetadata( evaluation, pool.build() );
    }

    /**
     * Builds a {@link PoolMetadata} from a prescribed input source and an override {@link TimeWindowOuter}.
     * 
     * @param input the source metadata
     * @param timeWindow the new time window
     * @return a {@link PoolMetadata} object
     * @throws NullPointerException if any required input is null
     */

    public static PoolMetadata of( PoolMetadata input, TimeWindowOuter timeWindow )
    {
        Objects.requireNonNull( input );

        Evaluation evaluation = input.getEvaluation();

        Pool.Builder pool = input.getPool().toBuilder();

        if ( Objects.nonNull( timeWindow ) )
        {
            pool.setTimeWindow( timeWindow.getTimeWindow() );
        }

        return new PoolMetadata( evaluation, pool.build() );
    }

    /**
     * Builds a {@link PoolMetadata} from a prescribed input source and an override {@link TimeScaleOuter}.
     * 
     * @param input the source metadata
     * @param timeScale the new time scale
     * @return a {@link PoolMetadata} object
     * @throws NullPointerException if the input is null
     */

    public static PoolMetadata of( PoolMetadata input, TimeScaleOuter timeScale )
    {
        Objects.requireNonNull( input );

        Evaluation evaluation = input.getEvaluation();

        Pool.Builder pool = input.getPool().toBuilder();

        if ( Objects.nonNull( timeScale ) )
        {
            pool.setTimeScale( timeScale.getTimeScale() );
        }

        return new PoolMetadata( evaluation, pool.build() );
    }

    /**
     * Builds a {@link PoolMetadata} from a prescribed input source and an override {@link TimeWindowOuter} and 
     * {@link TimeScaleOuter}.
     * 
     * @param input the source metadata
     * @param timeWindow the new time window
     * @param timeScale the new time scale
     * @return a {@link PoolMetadata} object
     * @throws NullPointerException if the input is null
     */

    public static PoolMetadata of( PoolMetadata input,
                                   TimeWindowOuter timeWindow,
                                   TimeScaleOuter timeScale )
    {
        Objects.requireNonNull( input );

        Evaluation evaluation = input.getEvaluation();

        Pool.Builder pool = input.getPool().toBuilder();

        if ( Objects.nonNull( timeScale ) )
        {
            pool.setTimeScale( timeScale.getTimeScale() );
        }

        if ( Objects.nonNull( timeWindow ) )
        {
            pool.setTimeWindow( timeWindow.getTimeWindow() );
        }

        return new PoolMetadata( evaluation, pool.build() );
    }

    /**
     * Builds a {@link PoolMetadata} from a prescribed input source and an override {@link TimeWindowOuter} and 
     * {@link OneOrTwoThresholds}.
     * 
     * @param input the source metadata
     * @param timeWindow the new time window
     * @param thresholds the thresholds
     * @return a {@link PoolMetadata} object
     * @throws NullPointerException if the input is null
     */

    public static PoolMetadata of( PoolMetadata input,
                                   TimeWindowOuter timeWindow,
                                   OneOrTwoThresholds thresholds )
    {
        Objects.requireNonNull( input );

        Evaluation evaluation = input.getEvaluation();

        Pool.Builder pool = input.getPool().toBuilder();

        if ( Objects.nonNull( thresholds ) )
        {
            pool.setEventThreshold( thresholds.first().getThreshold() );
            if ( thresholds.hasTwo() )
            {
                pool.setDecisionThreshold( thresholds.second().getThreshold() );
            }
        }

        if ( Objects.nonNull( timeWindow ) )
        {
            pool.setTimeWindow( timeWindow.getTimeWindow() );
        }

        return new PoolMetadata( evaluation, pool.build() );
    }

    @Override
    public int compareTo( PoolMetadata input )
    {
        Objects.requireNonNull( input, "Specify non-null metadata for comparison." );

        // Check measurement units, which are always available
        int returnMe = MessageUtilities.compare( this.getEvaluation(), input.getEvaluation() );

        if ( returnMe != 0 )
        {
            return returnMe;
        }

        return MessageUtilities.compare( this.getPool(), input.getPool() );
    }

    @Override
    public boolean equals( final Object o )
    {
        if ( ! ( o instanceof PoolMetadata ) )
        {
            return false;
        }

        PoolMetadata p = (PoolMetadata) o;

        return Objects.equals( this.getEvaluation(), p.getEvaluation() )
               && Objects.equals( this.getPool(), p.getPool() );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( this.getEvaluation(),
                             this.getPool() );
    }

    @Override
    public String toString()
    {
        // Use a limited subset of the most important/useful descriptors
        Evaluation innerEvaluation = this.getEvaluation();
        Pool innerPool = this.getPool();

        return new ToStringBuilder( this, ToStringStyle.SHORT_PREFIX_STYLE ).append( "poolId", innerPool.getPoolId() )
                                                                            .append( "leftDataName",
                                                                                     innerEvaluation.getLeftDataName() )
                                                                            .append( "rightDataName",
                                                                                     innerEvaluation.getRightDataName() )
                                                                            .append( "baselineDataName",
                                                                                     innerEvaluation.getBaselineDataName() )
                                                                            .append( "leftVariableName",
                                                                                     innerEvaluation.getLeftVariableName() )
                                                                            .append( "rightVariableName",
                                                                                     innerEvaluation.getRightVariableName() )
                                                                            .append( "baselineVariableName",
                                                                                     innerEvaluation.getBaselineVariableName() )
                                                                            .append( "isBaselinePool",
                                                                                     innerPool.getIsBaselinePool() )
                                                                            .append( "features", this.getFeatureGroup() )
                                                                            .append( "timeWindow",
                                                                                     this.getTimeWindow() )
                                                                            .append( "thresholds",
                                                                                     this.getThresholds() )
                                                                            .append( "timeScale",
                                                                                     this.getTimeScale() )
                                                                            .append( "measurementUnit",
                                                                                     this.getMeasurementUnit() )
                                                                            .build();
    }

    /**
     * Returns <code>true</code> if {@link #getTimeWindow()} returns non-null, otherwise <code>false</code>.
     * 
     * @return true if {@link #getTimeWindow()} returns non-null, false otherwise.
     */
    public boolean hasTimeWindow()
    {
        return Objects.nonNull( this.getTimeWindow() );
    }

    /**
     * Returns <code>true</code> if {@link #getThresholds()} returns non-null, otherwise <code>false</code>.
     * 
     * @return true if {@link #getThresholds()} returns non-null, false otherwise.
     */
    public boolean hasThresholds()
    {
        return Objects.nonNull( this.getThresholds() );
    }

    /**
     * Returns <code>true</code> if {@link #getTimeScale()} returns non-null, otherwise <code>false</code>.
     * 
     * @return true if {@link #getTimeScale()} returns non-null, false otherwise.
     */
    public boolean hasTimeScale()
    {
        return Objects.nonNull( this.getTimeScale() );
    }

    /**
     * Returns the measurement unit associated with the metric.
     * 
     * @return the measurement unit
     */

    public MeasurementUnit getMeasurementUnit()
    {
        String unit = this.getEvaluation().getMeasurementUnit();
        return MeasurementUnit.of( unit );
    }

    /**
     * Returns a {@link TimeWindowOuter} associated with the metadata or null.
     * 
     * @return a time window or null
     */

    public TimeWindowOuter getTimeWindow()
    {
        TimeWindowOuter outer = null;

        if ( this.getPool().hasTimeWindow() )
        {
            wres.statistics.generated.TimeWindow window = this.getPool()
                                                              .getTimeWindow();
            outer = new TimeWindowOuter.Builder( window ).build();
        }

        return outer;
    }

    /**
     * Returns the {@link FeatureTuple} associated with the metadata.
     * 
     * @return the feature tuples
     */

    public Set<FeatureTuple> getFeatureTuples()
    {
        return this.getPool()
                   .getGeometryTuplesList()
                   .stream()
                   .map( MessageFactory::parse )
                   .collect( Collectors.toUnmodifiableSet() );
    }
    
    /**
     * @return the feature group associated with the pool or null if the pool has no features
     */
    
    public FeatureGroup getFeatureGroup()
    {
        // Pretty print the feature tuples
        Set<FeatureTuple> featureTuples = this.getPool()
                                              .getGeometryTuplesList()
                                              .stream()
                                              .map( FeatureTuple::new )
                                              .collect( Collectors.toSet() );
        
        FeatureGroup featureGroup = null;
        
        if( ! featureTuples.isEmpty() )
        {
            featureGroup = FeatureGroup.of( this.getPool().getRegionName(), featureTuples );
        }
        
        return featureGroup;
    }

    /**
     * Returns a {@link OneOrTwoThresholds} associated with the metadata or null.
     * 
     * @return a set of thresholds or null
     */

    public OneOrTwoThresholds getThresholds()
    {
        OneOrTwoThresholds thresholds = null;

        if ( this.getPool().hasEventThreshold() )
        {
            wres.statistics.generated.Threshold event = this.getPool()
                                                            .getEventThreshold();
            ThresholdOuter eventOuter = new ThresholdOuter.Builder( event ).build();
            ThresholdOuter decisionOuter = null;

            if ( this.getPool().hasDecisionThreshold() )
            {
                wres.statistics.generated.Threshold decision = this.getPool()
                                                                   .getDecisionThreshold();
                decisionOuter = new ThresholdOuter.Builder( decision ).build();
            }

            thresholds = OneOrTwoThresholds.of( eventOuter, decisionOuter );
        }

        return thresholds;
    }

    /**
     * Returns a {@link TimeScaleOuter} associated with the metadata or null.
     * 
     * @return the time scale or null
     */

    public TimeScaleOuter getTimeScale()
    {
        TimeScaleOuter outer = null;
        if ( this.getPool().hasTimeScale() )
        {
            outer = TimeScaleOuter.of( this.getPool().getTimeScale() );
        }

        return outer;
    }

    /**
     * Returns the evaluation description.
     * 
     * @return the evaluation description.
     */

    public Evaluation getEvaluation()
    {
        return this.evaluation;
    }

    /**
     * Returns the pool description.
     * 
     * @return the pool description.
     */

    public Pool getPool()
    {
        return this.pool;
    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     * @throws NullPointerException if either input is null or the measurement unit is not set
     */

    private PoolMetadata( Evaluation evaluation, Pool pool )
    {
        Objects.requireNonNull( evaluation );
        Objects.requireNonNull( pool );

        this.evaluation = evaluation;
        this.pool = pool;

        this.validate();
    }

    /**
     * Validate the input.
     */

    private void validate()
    {
        String unit = this.getEvaluation().getMeasurementUnit();

        if ( unit.isBlank() )
        {
            throw new IllegalArgumentException( "The evaluation description must contain a valid measurement unit "
                                                + "in order to build the pool metadata." );
        }
    }

}
