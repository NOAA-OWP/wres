package wres.datamodel.sampledata;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import wres.config.generated.LeftOrRightOrBaseline;
import wres.datamodel.DatasetIdentifier;
import wres.datamodel.FeatureTuple;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.datamodel.time.TimeWindowOuter;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.Pool;

/**
 * An immutable store of metadata associated with {@link SampleData}.
 * 
 * @author james.brown@hydrosolved.com
 */
public class SampleMetadata implements Comparable<SampleMetadata>
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

    public static SampleMetadata of( Evaluation evaluation, Pool pool )
    {
        return new SampleMetadata( evaluation, pool );
    }

    /**
     * Build a {@link SampleMetadata} object with a default {@link MeasurementUnit} of 
     * {@link MeasurementUnit#DIMENSIONLESS}.
     * 
     * @return a {@link SampleMetadata} object
     */

    public static SampleMetadata of()
    {
        Evaluation evaluation = Evaluation.newBuilder()
                                          .setMeasurementUnit( MeasurementUnit.DIMENSIONLESS )
                                          .build();

        Pool pool = Pool.getDefaultInstance();

        return new SampleMetadata( evaluation, pool );
    }

    /**
     * Build a {@link SampleMetadata} object with a sample size and a prescribed {@link MeasurementUnit}.
     * 
     * @param unit the required measurement unit
     * @return a {@link SampleMetadata} object
     * @throws NullPointerException if the input is null
     * @deprecated in favor of {@link #of(Evaluation, Pool)}, rather than construction from non-canonical types
     */
    @Deprecated( since = "4.3", forRemoval = true )
    public static SampleMetadata of( MeasurementUnit unit )
    {
        Objects.requireNonNull( unit );

        Evaluation evaluation = Evaluation.newBuilder()
                                          .setMeasurementUnit( unit.getUnit() )
                                          .build();

        Pool pool = Pool.getDefaultInstance();

        return new SampleMetadata( evaluation, pool );
    }

    /**
     * Build a {@link SampleMetadata} object with a prescribed {@link MeasurementUnit} and an optional 
     * {@link DatasetIdentifier}.
     * 
     * @param unit the required measurement unit
     * @param identifier an optional dataset identifier (may be null)
     * @return a {@link SampleMetadata} object
     * @throws NullPointerException if any required input is null
     * @deprecated in favor of {@link #of(Evaluation, Pool)}, rather than construction from non-canonical types
     */
    @Deprecated( since = "4.3", forRemoval = true )
    public static SampleMetadata of( MeasurementUnit unit, DatasetIdentifier identifier )
    {
        Evaluation evaluation = MessageFactory.parse( unit, identifier, null );

        FeatureTuple featureTuple = null;
        boolean isBaseline = false;
        if ( Objects.nonNull( identifier ) )
        {
            featureTuple = identifier.getFeatureTuple();

            if ( identifier.hasLeftOrRightOrBaseline()
                 && identifier.getLeftOrRightOrBaseline() == LeftOrRightOrBaseline.BASELINE )
            {
                isBaseline = true;
            }
        }

        Pool pool = MessageFactory.parse( featureTuple,
                                          null,
                                          null,
                                          null,
                                          isBaseline );

        return new SampleMetadata( evaluation, pool );
    }

    /**
     * Returns an instance from the inputs.
     * 
     * @param unit the required measurement unit
     * @param identifier an optional dataset identifier
     * @param timeWindow an optional time window
     * @param thresholds an optional set of thresholds
     * @return a metadata instance
     * @throws NullPointerException if any required input is null
     * @deprecated in favor of {@link #of(Evaluation, Pool)}, rather than construction from non-canonical types
     */
    @Deprecated( since = "4.3", forRemoval = true )
    public static SampleMetadata of( MeasurementUnit unit,
                                     DatasetIdentifier identifier,
                                     TimeWindowOuter timeWindow,
                                     OneOrTwoThresholds thresholds )
    {
        Evaluation evaluation = MessageFactory.parse( unit, identifier, null );
        Pool pool = MessageFactory.parse( identifier.getFeatureTuple(), timeWindow, null, thresholds, false );

        return new SampleMetadata( evaluation, pool );
    }

    /**
     * Builds a {@link SampleMetadata} from a prescribed input source and an override {@link OneOrTwoThresholds}.
     * 
     * @param input the source metadata
     * @param thresholds the thresholds
     * @return a {@link SampleMetadata} object
     * @throws NullPointerException if any required input is null
     */

    public static SampleMetadata of( SampleMetadata input, OneOrTwoThresholds thresholds )
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

        return new SampleMetadata( evaluation, pool.build() );
    }

    /**
     * Builds a {@link SampleMetadata} from a prescribed input source and an override {@link TimeWindowOuter}.
     * 
     * @param input the source metadata
     * @param timeWindow the new time window
     * @return a {@link SampleMetadata} object
     * @throws NullPointerException if any required input is null
     */

    public static SampleMetadata of( SampleMetadata input, TimeWindowOuter timeWindow )
    {
        Objects.requireNonNull( input );

        Evaluation evaluation = input.getEvaluation();

        Pool.Builder pool = input.getPool().toBuilder();

        if ( Objects.nonNull( timeWindow ) )
        {
            pool.setTimeWindow( timeWindow.getTimeWindow() );
        }

        return new SampleMetadata( evaluation, pool.build() );
    }

    /**
     * Builds a {@link SampleMetadata} from a prescribed input source and an override {@link TimeScaleOuter}.
     * 
     * @param input the source metadata
     * @param timeScale the new time scale
     * @return a {@link SampleMetadata} object
     * @throws NullPointerException if the input is null
     */

    public static SampleMetadata of( SampleMetadata input, TimeScaleOuter timeScale )
    {
        Objects.requireNonNull( input );

        Evaluation evaluation = input.getEvaluation();

        Pool.Builder pool = input.getPool().toBuilder();

        if ( Objects.nonNull( timeScale ) )
        {
            pool.setTimeScale( timeScale.getTimeScale() );
        }

        return new SampleMetadata( evaluation, pool.build() );
    }

    /**
     * Builds a {@link SampleMetadata} from a prescribed input source and an override {@link TimeWindowOuter} and 
     * {@link TimeScaleOuter}.
     * 
     * @param input the source metadata
     * @param timeWindow the new time window
     * @param timeScale the new time scale
     * @return a {@link SampleMetadata} object
     * @throws NullPointerException if the input is null
     */

    public static SampleMetadata of( SampleMetadata input,
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

        return new SampleMetadata( evaluation, pool.build() );
    }

    /**
     * Builds a {@link SampleMetadata} from a prescribed input source and an override {@link TimeWindowOuter} and 
     * {@link OneOrTwoThresholds}.
     * 
     * @param input the source metadata
     * @param timeWindow the new time window
     * @param thresholds the thresholds
     * @return a {@link SampleMetadata} object
     * @throws NullPointerException if the input is null
     */

    public static SampleMetadata of( SampleMetadata input,
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

        return new SampleMetadata( evaluation, pool.build() );
    }

    /**
     * Finds the union of the input, based on the {@link TimeWindowOuter}. All components of the input must be equal, 
     * except the {@link SampleMetadata#getTimeWindow()} and {@link SampleMetadata#getThresholds()}, otherwise an 
     * exception is thrown. See also {@link TimeWindowOuter#unionOf(List)}. No threshold information is represented in the 
     * union.
     * 
     * @param input the input metadata
     * @return the union of the input
     * @throws IllegalArgumentException if the input is empty
     * @throws NullPointerException if the input is null
     * @throws SampleMetadataException if the input contains metadata whose differences extend beyond the time windows and
     *            thresholds
     */

    public static SampleMetadata unionOf( List<SampleMetadata> input )
    {
        String nullString = "Cannot find the union of null metadata.";

        Objects.requireNonNull( input, nullString );

        if ( input.isEmpty() )
        {
            throw new IllegalArgumentException( "Cannot find the union of empty input." );
        }
        List<TimeWindowOuter> unionWindow = new ArrayList<>();

        // Test entry
        SampleMetadata test = input.get( 0 );

        // Validate for equivalence with the first entry and add window to list
        for ( SampleMetadata next : input )
        {
            Objects.requireNonNull( next, nullString );

            if ( !next.equalsWithoutTimeWindowOrThresholds( test ) )
            {
                throw new SampleMetadataException( "Only the time window and thresholds can differ when finding the "
                                                   + "union of metadata." );
            }
            if ( next.hasTimeWindow() )
            {
                unionWindow.add( next.getTimeWindow() );
            }
        }

        // Remove any threshold information from the result
        test = SampleMetadata.of( test, (OneOrTwoThresholds) null );

        if ( !unionWindow.isEmpty() )
        {
            test = SampleMetadata.of( test, TimeWindowOuter.unionOf( unionWindow ) );
        }
        return test;
    }

    @Override
    public int compareTo( SampleMetadata input )
    {
        Objects.requireNonNull( input, "Specify non-null metadata for comparison." );

        // Check measurement units, which are always available
        int returnMe = this.getMeasurementUnit().compareTo( input.getMeasurementUnit() );
        if ( returnMe != 0 )
        {
            return returnMe;
        }

        // Check identifier via the string representation
        returnMe = Objects.compare( this.getIdentifier() + "", input.getIdentifier() + "", Comparator.naturalOrder() );
        if ( returnMe != 0 )
        {
            return returnMe;
        }

        // Check the time window
        Comparator<TimeWindowOuter> compareWindows = Comparator.nullsFirst( Comparator.naturalOrder() );
        returnMe = Objects.compare( this.getTimeWindow(), input.getTimeWindow(), compareWindows );
        if ( returnMe != 0 )
        {
            return returnMe;
        }

        // Check the thresholds
        Comparator<OneOrTwoThresholds> compareThresholds = Comparator.nullsFirst( Comparator.naturalOrder() );
        return Objects.compare( this.getThresholds(), input.getThresholds(), compareThresholds );
    }

    @Override
    public boolean equals( final Object o )
    {
        if ( ! ( o instanceof SampleMetadata ) )
        {
            return false;
        }
        SampleMetadata p = (SampleMetadata) o;
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
        return new ToStringBuilder( this, ToStringStyle.SHORT_PREFIX_STYLE )
                                                                            .append( "datasetIdentifier",
                                                                                     this.getIdentifier() )
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
     * Returns <code>true</code> if the input is equal to the current {@link SampleMetadata} without considering the 
     * {@link #getTimeWindow()} or {@link #getThresholds()}.
     * 
     * @param input the input metadata
     * @return true if the input is equal to the current metadata, without considering the time window or thresholds
     */
    public boolean equalsWithoutTimeWindowOrThresholds( final SampleMetadata input )
    {
        if ( Objects.isNull( input ) )
        {
            return false;
        }

        // Adjust the pools to remove the time window and thresholds
        Pool adjustedPoolThis = this.getPool()
                                    .toBuilder()
                                    .clearTimeWindow()
                                    .clearEventThreshold()
                                    .clearDecisionThreshold()
                                    .build();

        Pool adjustedPoolIn = input.getPool()
                                   .toBuilder()
                                   .clearTimeWindow()
                                   .clearEventThreshold()
                                   .clearDecisionThreshold()
                                   .build();

        return Objects.equals( input.getEvaluation(), this.getEvaluation() )
               && Objects.equals( adjustedPoolIn, adjustedPoolThis );
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
     * Returns an optional dataset identifier or null.
     * 
     * @return an identifier or null
     * @deprecated makes too many assumptions in mapping from the canonical form. Obtain the canonical contents 
     *            directly.
     */
    @Deprecated( since = "4.3", forRemoval = true )
    public DatasetIdentifier getIdentifier()
    {
        DatasetIdentifier returnMe = null;

        // For now, only one GeometryTuple per DatasetIdentifier
        // TODO: allow a DatasetIdentifier to contain N GeometryTuple
        FeatureTuple localLocation = null;

        if ( this.getPool().getGeometryTuplesCount() > 0 )
        {
            localLocation = MessageFactory.parse( this.getPool().getGeometryTuples( 0 ) );
        }

        LeftOrRightOrBaseline localContext = LeftOrRightOrBaseline.RIGHT;
        String variableName = null;
        String scenario = null;
        String baselineScenario = null;

        if ( !this.getEvaluation().getVariableName().isBlank() )
        {
            variableName = this.getEvaluation().getVariableName();
        }

        if ( !this.getEvaluation().getRightSourceName().isBlank() )
        {
            scenario = this.getEvaluation().getRightSourceName();
        }

        if ( !this.getEvaluation().getBaselineSourceName().isBlank() )
        {
            baselineScenario = this.getEvaluation().getBaselineSourceName();
        }

        if ( this.getPool().getIsBaselinePool() )
        {
            localContext = LeftOrRightOrBaseline.BASELINE;
        }

        if ( Objects.nonNull( localLocation ) || Objects.nonNull( variableName )
             || Objects.nonNull( scenario )
             || Objects.nonNull( baselineScenario ) )
        {
            returnMe = DatasetIdentifier.of( localLocation, variableName, scenario, baselineScenario, localContext );
        }

        return returnMe;
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

    private SampleMetadata( Evaluation evaluation, Pool pool )
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
            throw new IllegalArgumentException( "Specify a valid measurement unit from which to build the metadata." );
        }
    }

}
