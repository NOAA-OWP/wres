package wres.datamodel.pools;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import net.jcip.annotations.Immutable;

import wres.config.yaml.components.ThresholdType;
import wres.datamodel.messages.EvaluationStatusMessage;
import wres.datamodel.messages.MessageUtilities;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.FeatureGroup;
import wres.datamodel.space.FeatureTuple;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.datamodel.time.TimeWindowOuter;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.GeometryTuple;
import wres.statistics.generated.Pool;
import wres.statistics.generated.Pool.EnsembleAverageType;

/**
 * <p>An immutable store of metadata that describes a {@link Pool}.
 *
 * <p>TODO: eliminate the evaluation description from this class. This would be aided by integrating the measurement
 * units into the pool description and deprecating the corresponding attribute of the evaluation description.
 *
 * @author James Brown
 */

@Immutable
public class PoolMetadata implements Comparable<PoolMetadata>
{
    /** A description of the evaluation. */
    private final Evaluation evaluation;

    /** A description of the pool to which the sample data belongs. */
    private final Pool pool;

    /** A list of user-facing evaluation status events encountered while building this pool. These messages are not 
     * part of the pool description and should not be considered when testing for equality, for example. TODO: publish 
     * these messages at source: #100560.*/
    private final List<EvaluationStatusMessage> statusEvents;

    /** The wrapped measurement unit for convenient access. */
    private final MeasurementUnit measurementUnit;

    /** The wrapped timescale for convenient access. */
    private final TimeScaleOuter timeScale;

    /** The wrapped feature group for convenient access. */
    private final FeatureGroup featureGroup;

    /** The wrapped thresholds for convenient access. */
    private final OneOrTwoThresholds thresholds;

    /** The wrapped time window for convenient access. */
    private final TimeWindowOuter timeWindow;

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
        return new PoolMetadata( evaluation, pool, List.of() );
    }

    /**
     * Creates an instance from an {@link Evaluation} and a {@link Pool}.
     *
     * @param evaluation the evaluation
     * @param pool the pool
     * @param statusEvents a list of zero or more evaluation status events encountered while creating the pool
     * @return an instance
     * @throws NullPointerException if any input is null.
     */

    public static PoolMetadata of( Evaluation evaluation, Pool pool, List<EvaluationStatusMessage> statusEvents )
    {
        return new PoolMetadata( evaluation, pool, statusEvents );
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

        return new PoolMetadata( evaluation, pool, List.of() );
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

        return new PoolMetadata( evaluation, pool, List.of() );
    }

    /**
     * Builds a {@link PoolMetadata} from a prescribed input source and an override {@link OneOrTwoThresholds}.
     *
     * @param input the source metadata
     * @param thresholds the thresholds
     * @return the pool metadata built from the combined inputs
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

        return new PoolMetadata( evaluation, pool.build(), input.getEvaluationStatusEvents() );
    }

    /**
     * Builds a {@link PoolMetadata} from a prescribed input source and an override {@link TimeWindowOuter}.
     *
     * @param input the source metadata
     * @param timeWindow the new time window
     * @return the pool metadata built from the combined inputs
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

        return new PoolMetadata( evaluation, pool.build(), input.getEvaluationStatusEvents() );
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

        return new PoolMetadata( evaluation, pool.build(), input.getEvaluationStatusEvents() );
    }

    /**
     * Builds a {@link PoolMetadata} from a prescribed input source and an override {@link EnsembleAverageType}.
     *
     * @param input the source metadata
     * @param ensembleAverageType the new ensemble average type
     * @return a {@link PoolMetadata} object
     * @throws NullPointerException if the input is null
     */

    public static PoolMetadata of( PoolMetadata input, EnsembleAverageType ensembleAverageType )
    {
        Objects.requireNonNull( input );

        Evaluation evaluation = input.getEvaluation();

        Pool.Builder pool = input.getPool().toBuilder();

        if ( Objects.nonNull( ensembleAverageType ) )
        {
            pool.setEnsembleAverageType( ensembleAverageType );
        }

        return new PoolMetadata( evaluation, pool.build(), input.getEvaluationStatusEvents() );
    }

    /**
     * Builds a {@link PoolMetadata} from a prescribed input source and an override {@link TimeWindowOuter} and 
     * {@link TimeScaleOuter}.
     *
     * @param input the source metadata
     * @param timeWindow the new time window
     * @param timeScale the new time scale
     * @return the pool metadata built from the combined inputs
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

        return new PoolMetadata( evaluation, pool.build(), input.getEvaluationStatusEvents() );
    }

    /**
     * Builds a {@link PoolMetadata} from a prescribed input source and an override {@link TimeWindowOuter} and 
     * {@link OneOrTwoThresholds}.
     *
     * @param input the source metadata
     * @param timeWindow the new time window
     * @param thresholds the thresholds
     * @return the pool metadata built from the combined inputs
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

        return new PoolMetadata( evaluation, pool.build(), input.getEvaluationStatusEvents() );
    }

    /**
     * Builds a {@link PoolMetadata} from a prescribed input source and an override {@link FeatureGroup}.
     *
     * @param input the source metadata
     * @param featureGroup the new feature group
     * @return the pool metadata built from the combined inputs
     * @throws NullPointerException if the input is null
     */

    public static PoolMetadata of( PoolMetadata input, FeatureGroup featureGroup )
    {
        Objects.requireNonNull( input );

        Evaluation evaluation = input.getEvaluation();

        Pool.Builder pool = input.getPool().toBuilder();

        if ( Objects.nonNull( featureGroup ) )
        {
            pool.clearGeometryTuples();
            Set<FeatureTuple> featureTuples = featureGroup.getFeatures();
            Set<GeometryTuple> geometryTuples = featureTuples.stream()
                                                             .map( FeatureTuple::getGeometryTuple )
                                                             .collect( Collectors.toSet() );
            pool.addAllGeometryTuples( geometryTuples );

            if ( Objects.nonNull( featureGroup.getName() ) )
            {
                pool.setRegionName( featureGroup.getName() );
            }
            else
            {
                pool.clearRegionName();
            }

            pool.setGeometryGroup( featureGroup.getGeometryGroup() );
        }
        else
        {
            pool.clearGeometryGroup();
        }

        return new PoolMetadata( evaluation, pool.build(), input.getEvaluationStatusEvents() );
    }

    @Override
    public int compareTo( PoolMetadata input )
    {
        // Check measurement units, which are always available
        int returnMe = MessageUtilities.compare( this.getEvaluation(), input.getEvaluation() );

        if ( returnMe != 0 )
        {
            return returnMe;
        }

        return MessageUtilities.compare( this.getPool(), input.getPool() );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }

        if ( !( o instanceof PoolMetadata p ) )
        {
            return false;
        }

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
                                                                            .append( "features",
                                                                                     this.getFeatureGroup() )
                                                                            .append( "timeWindow",
                                                                                     this.getTimeWindow() )
                                                                            .append( "thresholds",
                                                                                     this.getThresholds() )
                                                                            .append( "timeScale",
                                                                                     this.getTimeScale() )
                                                                            .append( "measurementUnit",
                                                                                     this.getMeasurementUnit() )
                                                                            .append( "ensembleAverageType",
                                                                                     innerPool.getEnsembleAverageType() )
                                                                            .build();
    }

    /**
     * Returns <code>true</code> if {@link #getTimeWindow()} returns non-null, otherwise <code>false</code>.
     *
     * @return true if {@link #getTimeWindow()} returns non-null, false otherwise.
     */
    public boolean hasTimeWindow()
    {
        return Objects.nonNull( this.timeWindow );
    }

    /**
     * Returns <code>true</code> if {@link #getThresholds()} returns non-null, otherwise <code>false</code>.
     *
     * @return true if {@link #getThresholds()} returns non-null, false otherwise.
     */
    public boolean hasThresholds()
    {
        return Objects.nonNull( this.thresholds );
    }

    /**
     * Returns <code>true</code> if {@link #getTimeScale()} returns non-null, otherwise <code>false</code>.
     *
     * @return true if {@link #getTimeScale()} returns non-null, false otherwise.
     */
    public boolean hasTimeScale()
    {
        return Objects.nonNull( this.timeScale );
    }

    /**
     * Returns <code>true</code> if {@link #getFeatureGroup()} returns non-null, otherwise <code>false</code>.
     *
     * @return true if {@link #getFeatureGroup()} returns non-null, false otherwise.
     */
    public boolean hasFeatureGroup()
    {
        return Objects.nonNull( this.featureGroup );
    }

    /**
     * Returns the measurement unit associated with the metric.
     *
     * @return the measurement unit
     */

    public MeasurementUnit getMeasurementUnit()
    {
        return this.measurementUnit;
    }

    /**
     * Returns a {@link TimeWindowOuter} associated with the metadata or null.
     *
     * @return a time window or null
     */

    public TimeWindowOuter getTimeWindow()
    {
        return this.timeWindow;
    }

    /**
     * Returns the {@link FeatureTuple} associated with the metadata.
     *
     * @return the feature tuples
     */

    public Set<FeatureTuple> getFeatureTuples()
    {
        if ( this.hasFeatureGroup() )
        {
            return this.featureGroup.getFeatures();
        }

        return Set.of();
    }

    /**
     * @return the feature group associated with the pool or null if the pool has no features
     */

    public FeatureGroup getFeatureGroup()
    {
        return this.featureGroup;
    }

    /**
     * Returns a {@link OneOrTwoThresholds} associated with the metadata or null.
     *
     * @return a set of thresholds or null
     */

    public OneOrTwoThresholds getThresholds()
    {
        return this.thresholds;
    }

    /**
     * Returns a {@link TimeScaleOuter} associated with the metadata or null.
     *
     * @return the time scale or null
     */

    public TimeScaleOuter getTimeScale()
    {
        return this.timeScale;
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
     * Returns a list of evaluation status events encountered while building the pool attached to this metadata.
     *
     * @return the evaluation status events
     */

    public List<EvaluationStatusMessage> getEvaluationStatusEvents()
    {
        return this.statusEvents; // Immutable on construction
    }

    /**
     * Hidden constructor.
     *
     * @param evaluation the evaluation
     * @param pool the pool
     * @param statusEvents a list of zero or more events encountered while building the pool
     * @throws NullPointerException if any input is null or the measurement unit is not set
     */

    private PoolMetadata( Evaluation evaluation, Pool pool, List<EvaluationStatusMessage> statusEvents )
    {
        Objects.requireNonNull( evaluation );
        Objects.requireNonNull( pool );
        Objects.requireNonNull( statusEvents );

        this.evaluation = evaluation;
        this.pool = pool;
        this.statusEvents = statusEvents;

        // Validate the state
        this.validate();

        // Set the wrapped parameters for convenient access
        String unit = this.getEvaluation()
                          .getMeasurementUnit();

        this.measurementUnit = MeasurementUnit.of( unit );

        TimeScaleOuter timeScaleInner = null;
        if ( this.getPool().hasTimeScale() )
        {
            timeScaleInner = TimeScaleOuter.of( this.getPool().getTimeScale() );
        }

        this.timeScale = timeScaleInner;

        TimeWindowOuter timeWindowInner = null;

        if ( this.getPool().hasTimeWindow() )
        {
            wres.statistics.generated.TimeWindow window = this.getPool()
                                                              .getTimeWindow();
            timeWindowInner = TimeWindowOuter.of( window );
        }

        this.timeWindow = timeWindowInner;

        Set<FeatureTuple> featureTuples = this.getPool()
                                              .getGeometryGroup()
                                              .getGeometryTuplesList()
                                              .stream()
                                              .map( FeatureTuple::of )
                                              .collect( Collectors.toSet() );

        FeatureGroup featureGroupInner = null;

        if ( !featureTuples.isEmpty() )
        {
            featureGroupInner = FeatureGroup.of( this.getPool().getGeometryGroup() );
        }

        this.featureGroup = featureGroupInner;

        OneOrTwoThresholds thresholdsInner = null;

        if ( this.getPool()
                 .hasEventThreshold() )
        {
            wres.statistics.generated.Threshold event = this.getPool()
                                                            .getEventThreshold();

            ThresholdOuter eventOuter = ThresholdOuter.of( event );
            ThresholdOuter decisionOuter = null;

            if ( this.getPool()
                     .hasDecisionThreshold() )
            {
                wres.statistics.generated.Threshold decision = this.getPool()
                                                                   .getDecisionThreshold();
                decisionOuter = ThresholdOuter.of( decision, ThresholdType.PROBABILITY_CLASSIFIER );
            }

            thresholdsInner = OneOrTwoThresholds.of( eventOuter, decisionOuter );
        }

        this.thresholds = thresholdsInner;
    }

    /**
     * Validate the input.
     */

    private void validate()
    {
        String unit = this.getEvaluation()
                          .getMeasurementUnit();

        if ( unit.isBlank() )
        {
            throw new IllegalArgumentException( "The evaluation description must contain a valid measurement unit "
                                                + "in order to build the pool metadata." );
        }
    }
}
