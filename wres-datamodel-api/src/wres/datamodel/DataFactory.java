package wres.datamodel;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.MetricConstants.ScoreOutputGroup;
import wres.datamodel.Threshold.Operator;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.DichotomousPairs;
import wres.datamodel.inputs.pairs.DiscreteProbabilityPairs;
import wres.datamodel.inputs.pairs.EnsemblePairs;
import wres.datamodel.inputs.pairs.MulticategoryPairs;
import wres.datamodel.inputs.pairs.PairOfBooleans;
import wres.datamodel.inputs.pairs.PairOfDoubleAndVectorOfDoubles;
import wres.datamodel.inputs.pairs.PairOfDoubles;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.inputs.pairs.TimeSeriesOfEnsemblePairs;
import wres.datamodel.inputs.pairs.TimeSeriesOfSingleValuedPairs;
import wres.datamodel.inputs.pairs.builders.RegularTimeSeriesOfEnsemblePairsBuilder;
import wres.datamodel.inputs.pairs.builders.RegularTimeSeriesOfSingleValuedPairsBuilder;
import wres.datamodel.metadata.Metadata;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.metadata.ReferenceTime;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.outputs.BoxPlotOutput;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.datamodel.outputs.DurationScoreOutput;
import wres.datamodel.outputs.MapKey;
import wres.datamodel.outputs.MatrixOutput;
import wres.datamodel.outputs.MetricOutput;
import wres.datamodel.outputs.MetricOutputException;
import wres.datamodel.outputs.MetricOutputForProjectByTimeAndThreshold;
import wres.datamodel.outputs.MetricOutputForProjectByTimeAndThreshold.MetricOutputForProjectByTimeAndThresholdBuilder;
import wres.datamodel.outputs.MetricOutputMapByMetric;
import wres.datamodel.outputs.MetricOutputMapByTimeAndThreshold;
import wres.datamodel.outputs.MetricOutputMultiMapByTimeAndThreshold;
import wres.datamodel.outputs.MetricOutputMultiMapByTimeAndThreshold.MetricOutputMultiMapByTimeAndThresholdBuilder;
import wres.datamodel.outputs.MultiVectorOutput;
import wres.datamodel.outputs.PairedOutput;

/**
 * A factory class for producing datasets associated with verification metrics.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public interface DataFactory
{

    /**
     * Convenience method that returns a {@link Pair} to map a {@link MetricOutput} by {@link TimeWindow} and
     * {@link Threshold}.
     * 
     * @param timeWindow the time window
     * @param threshold the threshold value
     * @param condition the threshold condition
     * @return a map key
     */

    default Pair<TimeWindow, Threshold> getMapKeyByTimeThreshold( final TimeWindow timeWindow,
                                                                  final Double threshold,
                                                                  final Operator condition )
    {
        return Pair.of( timeWindow, getThreshold( threshold, condition ) );
    }

    /**
     * Convenience method that returns a {@link Pair} to map a {@link MetricOutput} by {@link TimeWindow} and
     * {@link Threshold}.
     * 
     * @param timeWindow the time window
     * @param threshold the threshold value or lower bound of a {@link Operator#BETWEEN} condition
     * @param thresholdUpper the upper threshold of a {@link Operator#BETWEEN} or null
     * @param condition the threshold condition
     * @return a map key
     */

    default Pair<TimeWindow, Threshold> getMapKeyByTimeThreshold( final TimeWindow timeWindow,
                                                                  final Double threshold,
                                                                  final Double thresholdUpper,
                                                                  final Operator condition )
    {
        return Pair.of( timeWindow, getThreshold( threshold, thresholdUpper, condition ) );
    }

    /**
     * Returns {@link Threshold} from the specified input.
     * 
     * @param threshold the threshold value or lower bound
     * @param condition the threshold condition
     * @return a threshold
     */

    default Threshold getThreshold( final Double threshold, final Operator condition )
    {
        return getThreshold( threshold, null, condition );
    }

    /**
     * Returns {@link Threshold} from the specified input. The input must be in the unit interval, [0,1].
     * 
     * @param threshold the threshold value or lower bound
     * @param condition the threshold condition
     * @return a threshold
     */

    default Threshold getProbabilityThreshold( final Double threshold, final Operator condition )
    {
        return getProbabilityThreshold( threshold, null, condition );
    }

    /**
     * Returns a {@link Threshold} from the specified input
     * 
     * @param threshold the threshold value
     * @param probability the probability associated with the threshold
     * @param condition the threshold condition
     * @return a quantile
     */

    default Threshold getQuantileThreshold( final Double threshold,
                                            final Double probability,
                                            final Operator condition )
    {
        return getQuantileThreshold( threshold, null, probability, null, condition );
    }

    /**
     * Construct the dichotomous input without any pairs for a baseline.
     * 
     * @param pairs the verification pairs
     * @param meta the metadata
     * @return the pairs
     * @throws MetricInputException if the inputs are invalid
     */

    default DichotomousPairs ofDichotomousPairs( final List<VectorOfBooleans> pairs, final Metadata meta )
    {
        return ofDichotomousPairs( pairs, null, meta, null, null );
    }

    /**
     * Construct the dichotomous input from atomic {@link PairOfBooleans} without any pairs for a baseline.
     * 
     * @param pairs the verification pairs
     * @param meta the metadata
     * @return the pairs
     * @throws MetricInputException if the inputs are invalid
     */

    default DichotomousPairs ofDichotomousPairsFromAtomic( final List<PairOfBooleans> pairs, final Metadata meta )
    {
        return ofDichotomousPairsFromAtomic( pairs, null, meta, null, null );
    }

    /**
     * Construct the multicategory input without any pairs for a baseline.
     * 
     * @param pairs the verification pairs
     * @param meta the metadata
     * @return the pairs
     * @throws MetricInputException if the inputs are invalid
     */

    default MulticategoryPairs ofMulticategoryPairs( final List<VectorOfBooleans> pairs, final Metadata meta )
    {
        return ofMulticategoryPairs( pairs, null, meta, null, null );
    }

    /**
     * Construct the discrete probability input without any pairs for a baseline.
     * 
     * @param pairs the discrete probability pairs
     * @param meta the metadata
     * @throws MetricInputException if the inputs are invalid
     * @return the pairs
     */

    default DiscreteProbabilityPairs ofDiscreteProbabilityPairs( final List<PairOfDoubles> pairs, final Metadata meta )
    {
        return ofDiscreteProbabilityPairs( pairs, null, meta, null, null );
    }

    /**
     * Construct the single-valued input without any pairs for a baseline.
     * 
     * @param pairs the single-valued pairs
     * @param meta the metadata
     * @return the pairs
     * @throws MetricInputException if the inputs are invalid
     */

    default SingleValuedPairs ofSingleValuedPairs( final List<PairOfDoubles> pairs, final Metadata meta )
    {
        return ofSingleValuedPairs( pairs, null, meta, null, null );
    }

    /**
     * Construct the ensemble input without any pairs for a baseline.
     * 
     * @param pairs the ensemble pairs
     * @param meta the metadata
     * @return the pairs
     * @throws MetricInputException if the inputs are invalid
     */

    default EnsemblePairs ofEnsemblePairs( final List<PairOfDoubleAndVectorOfDoubles> pairs, final Metadata meta )
    {
        return ofEnsemblePairs( pairs, null, meta, null, null );
    }

    /**
     * Construct the dichotomous input without any pairs for a baseline.
     * 
     * @param pairs the verification pairs
     * @param meta the metadata
     * @param climatology an optional climatological dataset (may be null)
     * @return the pairs
     * @throws MetricInputException if the inputs are invalid
     */

    default DichotomousPairs ofDichotomousPairs( final List<VectorOfBooleans> pairs,
                                                 final Metadata meta,
                                                 final VectorOfDoubles climatology )
    {
        return ofDichotomousPairs( pairs, null, meta, null, climatology );
    }

    /**
     * Construct the dichotomous input from atomic {@link PairOfBooleans} without any pairs for a baseline.
     * 
     * @param pairs the verification pairs
     * @param meta the metadata
     * @param climatology an optional climatological dataset (may be null)
     * @return the pairs
     * @throws MetricInputException if the inputs are invalid
     */

    default DichotomousPairs ofDichotomousPairsFromAtomic( final List<PairOfBooleans> pairs,
                                                           final Metadata meta,
                                                           final VectorOfDoubles climatology )
    {
        return ofDichotomousPairsFromAtomic( pairs, null, meta, null, climatology );
    }

    /**
     * Construct the multicategory input without any pairs for a baseline.
     * 
     * @param pairs the verification pairs
     * @param meta the metadata
     * @param climatology an optional climatological dataset (may be null)
     * @return the pairs
     * @throws MetricInputException if the inputs are invalid
     */

    default MulticategoryPairs ofMulticategoryPairs( final List<VectorOfBooleans> pairs,
                                                     final Metadata meta,
                                                     final VectorOfDoubles climatology )
    {
        return ofMulticategoryPairs( pairs, null, meta, null, climatology );
    }

    /**
     * Construct the discrete probability input without any pairs for a baseline.
     * 
     * @param pairs the discrete probability pairs
     * @param meta the metadata
     * @param climatology an optional climatological dataset (may be null)
     * @throws MetricInputException if the inputs are invalid
     * @return the pairs
     */

    default DiscreteProbabilityPairs ofDiscreteProbabilityPairs( final List<PairOfDoubles> pairs,
                                                                 final Metadata meta,
                                                                 final VectorOfDoubles climatology )
    {
        return ofDiscreteProbabilityPairs( pairs, null, meta, null, climatology );
    }

    /**
     * Construct the single-valued input without any pairs for a baseline.
     * 
     * @param pairs the single-valued pairs
     * @param meta the metadata
     * @param climatology an optional climatological dataset (may be null)
     * @return the pairs
     * @throws MetricInputException if the inputs are invalid
     */

    default SingleValuedPairs ofSingleValuedPairs( final List<PairOfDoubles> pairs,
                                                   final Metadata meta,
                                                   final VectorOfDoubles climatology )
    {
        return ofSingleValuedPairs( pairs, null, meta, null, climatology );
    }

    /**
     * Construct the ensemble input without any pairs for a baseline.
     * 
     * @param pairs the ensemble pairs
     * @param meta the metadata
     * @param climatology an optional climatological dataset (may be null)
     * @return the pairs
     * @throws MetricInputException if the inputs are invalid
     */

    default EnsemblePairs ofEnsemblePairs( final List<PairOfDoubleAndVectorOfDoubles> pairs,
                                           final Metadata meta,
                                           final VectorOfDoubles climatology )
    {
        return ofEnsemblePairs( pairs, null, meta, null, climatology );
    }

    /**
     * Construct the single-valued input with a baseline.
     * 
     * @param pairs the main verification pairs
     * @param basePairs the baseline pairs (may be null)
     * @param mainMeta the metadata for the main pairs
     * @param baselineMeta the metadata for the baseline pairs (may be null, if the basePairs are null)
     * @return the pairs
     * @throws MetricInputException if the inputs are invalid
     */

    default SingleValuedPairs ofSingleValuedPairs( final List<PairOfDoubles> pairs,
                                                   final List<PairOfDoubles> basePairs,
                                                   final Metadata mainMeta,
                                                   final Metadata baselineMeta )
    {
        return ofSingleValuedPairs( pairs, basePairs, mainMeta, baselineMeta, null );
    }

    /**
     * Construct the ensemble input with a baseline.
     * 
     * @param pairs the main verification pairs
     * @param basePairs the baseline pairs (may be null)
     * @param mainMeta the metadata for the main pairs
     * @param baselineMeta the metadata for the baseline pairs (may be null, if the basePairs are null)
     * @return the pairs
     * @throws MetricInputException if the inputs are invalid
     */

    default EnsemblePairs ofEnsemblePairs( final List<PairOfDoubleAndVectorOfDoubles> pairs,
                                           final List<PairOfDoubleAndVectorOfDoubles> basePairs,
                                           final Metadata mainMeta,
                                           final Metadata baselineMeta )
    {
        return ofEnsemblePairs( pairs, basePairs, mainMeta, baselineMeta, null );
    }

    /**
     * Construct the multicategory input without any pairs for a baseline.
     * 
     * @param pairs the main verification pairs
     * @param basePairs the baseline pairs (may be null)
     * @param mainMeta the metadata for the main pairs
     * @param baselineMeta the metadata for the baseline pairs (may be null, if the basePairs are null)
     * @return the pairs
     * @throws MetricInputException if the inputs are invalid
     */

    default MulticategoryPairs ofMulticategoryPairs( final List<VectorOfBooleans> pairs,
                                                     final List<VectorOfBooleans> basePairs,
                                                     final Metadata mainMeta,
                                                     final Metadata baselineMeta )
    {
        return ofMulticategoryPairs( pairs, basePairs, mainMeta, baselineMeta, null );
    }

    /**
     * Construct the discrete probability input with a baseline.
     * 
     * @param pairs the main verification pairs
     * @param basePairs the baseline pairs (may be null)
     * @param mainMeta the metadata for the main pairs
     * @param baselineMeta the metadata for the baseline pairs (may be null, if the basePairs are null)
     * @throws MetricInputException if the inputs are invalid
     * @return the pairs
     */

    default DiscreteProbabilityPairs ofDiscreteProbabilityPairs( final List<PairOfDoubles> pairs,
                                                                 final List<PairOfDoubles> basePairs,
                                                                 final Metadata mainMeta,
                                                                 final Metadata baselineMeta )
    {
        return ofDiscreteProbabilityPairs( pairs, basePairs, mainMeta, baselineMeta, null );
    }

    /**
     * Construct the dichotomous input with pairs for a baseline.
     * 
     * @param pairs the main verification pairs
     * @param basePairs the baseline pairs (may be null)
     * @param mainMeta the metadata for the main pairs
     * @param baselineMeta the metadata for the baseline pairs (may be null, if the basePairs are null)
     * @return the pairs
     * @throws MetricInputException if the inputs are invalid
     */

    default DichotomousPairs ofDichotomousPairs( final List<VectorOfBooleans> pairs,
                                                 final List<VectorOfBooleans> basePairs,
                                                 final Metadata mainMeta,
                                                 final Metadata baselineMeta )
    {
        return ofDichotomousPairs( pairs, basePairs, mainMeta, baselineMeta, null );
    }

    /**
     * Constructs a {@link TimeWindow} that comprises the intersection of two timelines, namely the UTC timeline and
     * forecast lead time. Each timeline is partitioned within the input parameters, and a {@link TimeWindow} forms
     * the intersection of each partition, i.e. contains elements that are common to each partition.  
     * 
     * @param earliestTime the earliest time
     * @param latestTime the latest time
     * @param referenceTime the reference time system
     * @param earliestLead the earliest lead time
     * @param latestLead the latest lead time
     * @return a time window
     * @throws IllegalArgumentException if the latestTime is before (i.e. smaller than) the earliestTime or the 
     *            latestLeadTime is before (i.e. smaller than) the earliestLeadTime.  
     */

    default TimeWindow ofTimeWindow( Instant earliestTime,
                                     Instant latestTime,
                                     ReferenceTime referenceTime,
                                     Duration earliestLead,
                                     Duration latestLead )
    {
        return TimeWindow.of( earliestTime, latestTime, referenceTime, earliestLead, latestLead );
    }

    /**
     * <p>Constructs a {@link TimeWindow} that comprises the intersection of two timelines, namely the UTC timeline and
     * forecast lead time. Here, the forecast lead time is zero.</p>
     * 
     * <p>Also see {@link #ofTimeWindow(Instant, Instant, ReferenceTime, Duration, Duration)}.</p>
     * 
     * @param earliestTime the earliest time
     * @param latestTime the latest time
     * @param referenceTime the reference time system
     * @return a time window
     * @throws IllegalArgumentException if the latestTime is before (i.e. smaller than) the earliestTime
     */

    default TimeWindow ofTimeWindow( Instant earliestTime,
                                     Instant latestTime,
                                     ReferenceTime referenceTime )
    {
        return TimeWindow.of( earliestTime, latestTime, referenceTime, Duration.ofHours( 0 ) );
    }

    /**
     * Returns a {@link MetadataFactory} for building {@link Metadata}.
     * 
     * @return an instance of {@link MetadataFactory}
     */

    MetadataFactory getMetadataFactory();

    /**
     * Returns a {@link Slicer} for slicing data.
     * 
     * @return a {@link Slicer}
     */

    Slicer getSlicer();

    /**
     * Construct the single-valued input with a baseline.
     * 
     * @param pairs the main verification pairs
     * @param basePairs the baseline pairs (may be null)
     * @param mainMeta the metadata for the main pairs
     * @param baselineMeta the metadata for the baseline pairs (may be null, if the basePairs are null)
     * @param climatology an optional climatological dataset (may be null)
     * @return the pairs
     * @throws MetricInputException if the inputs are invalid
     */

    SingleValuedPairs ofSingleValuedPairs( final List<PairOfDoubles> pairs,
                                           final List<PairOfDoubles> basePairs,
                                           final Metadata mainMeta,
                                           final Metadata baselineMeta,
                                           final VectorOfDoubles climatology );

    /**
     * Construct the ensemble input with a baseline.
     * 
     * @param pairs the main verification pairs
     * @param basePairs the baseline pairs (may be null)
     * @param mainMeta the metadata for the main pairs
     * @param baselineMeta the metadata for the baseline pairs (may be null, if the basePairs are null)
     * @param climatology an optional climatological dataset (may be null)
     * @return the pairs
     * @throws MetricInputException if the inputs are invalid
     */

    EnsemblePairs ofEnsemblePairs( final List<PairOfDoubleAndVectorOfDoubles> pairs,
                                   final List<PairOfDoubleAndVectorOfDoubles> basePairs,
                                   final Metadata mainMeta,
                                   final Metadata baselineMeta,
                                   final VectorOfDoubles climatology );

    /**
     * Construct the multicategory input without any pairs for a baseline.
     * 
     * @param pairs the main verification pairs
     * @param basePairs the baseline pairs (may be null)
     * @param mainMeta the metadata for the main pairs
     * @param baselineMeta the metadata for the baseline pairs (may be null, if the basePairs are null)
     * @param climatology an optional climatological dataset (may be null)
     * @return the pairs
     * @throws MetricInputException if the inputs are invalid
     */

    MulticategoryPairs ofMulticategoryPairs( final List<VectorOfBooleans> pairs,
                                             final List<VectorOfBooleans> basePairs,
                                             final Metadata mainMeta,
                                             final Metadata baselineMeta,
                                             final VectorOfDoubles climatology );

    /**
     * Construct the discrete probability input with a baseline.
     * 
     * @param pairs the main verification pairs
     * @param basePairs the baseline pairs (may be null)
     * @param mainMeta the metadata for the main pairs
     * @param baselineMeta the metadata for the baseline pairs (may be null, if the basePairs are null)
     * @param climatology an optional climatological dataset (may be null)
     * @throws MetricInputException if the inputs are invalid
     * @return the pairs
     */

    DiscreteProbabilityPairs ofDiscreteProbabilityPairs( final List<PairOfDoubles> pairs,
                                                         final List<PairOfDoubles> basePairs,
                                                         final Metadata mainMeta,
                                                         final Metadata baselineMeta,
                                                         final VectorOfDoubles climatology );

    /**
     * Construct the dichotomous input with pairs for a baseline.
     * 
     * @param pairs the main verification pairs
     * @param basePairs the baseline pairs (may be null)
     * @param mainMeta the metadata for the main pairs
     * @param baselineMeta the metadata for the baseline pairs (may be null, if the basePairs are null)
     * @param climatology an optional climatological dataset (may be null)
     * @return the pairs
     * @throws MetricInputException if the inputs are invalid
     */

    DichotomousPairs ofDichotomousPairs( final List<VectorOfBooleans> pairs,
                                         final List<VectorOfBooleans> basePairs,
                                         final Metadata mainMeta,
                                         final Metadata baselineMeta,
                                         final VectorOfDoubles climatology );

    /**
     * Construct the dichotomous input from atomic {@link PairOfBooleans} with pairs for a baseline.
     * 
     * @param pairs the main verification pairs
     * @param basePairs the baseline pairs (may be null)
     * @param mainMeta the metadata for the main pairs
     * @param baselineMeta the metadata for the baseline pairs (may be null, if the basePairs are null)
     * @param climatology an optional climatological dataset (may be null)
     * @return the pairs
     * @throws MetricInputException if the inputs are invalid
     */

    DichotomousPairs ofDichotomousPairsFromAtomic( final List<PairOfBooleans> pairs,
                                                   final List<PairOfBooleans> basePairs,
                                                   final Metadata mainMeta,
                                                   final Metadata baselineMeta,
                                                   final VectorOfDoubles climatology );

    /**
     * Return a {@link PairOfDoubles} from two double values.
     * 
     * @param left the left value
     * @param right the right value
     * @return the pair
     */

    PairOfDoubles pairOf( final double left, final double right );

    /**
     * Return a {@link PairOfBooleans} from two boolean values.
     * 
     * @param left the first value
     * @param right the second value
     * @return the pair
     */

    PairOfBooleans pairOf( final boolean left, final boolean right );

    /**
     * Return a {@link PairOfDoubleAndVectorOfDoubles} from a double value and a double vector of values.
     * 
     * @param left the first value
     * @param right the second value
     * @return the pair
     */

    PairOfDoubleAndVectorOfDoubles pairOf( final double left, final double[] right );

    /**
     * Return a {@link PairOfDoubleAndVectorOfDoubles} from a double value and a double vector of values.
     * 
     * @param left the first value
     * @param right the second value
     * @return the pair
     */

    PairOfDoubleAndVectorOfDoubles pairOf( final Double left, final Double[] right );

    /**
     * Return a {@link Pair} from two double vectors.
     * 
     * @param left the first value
     * @param right the second value
     * @return the pair
     */

    Pair<VectorOfDoubles, VectorOfDoubles> pairOf( final double[] left, final double[] right );

    /**
     * Return a {@link VectorOfDoubles} from a vector of doubles
     * 
     * @param vec the vector of doubles
     * @return the vector
     */

    VectorOfDoubles vectorOf( final double[] vec );

    /**
     * Return a {@link VectorOfDoubles} from a vector of doubles
     * 
     * @param vec the vector of doubles
     * @return the vector
     */

    VectorOfDoubles vectorOf( final Double[] vec );

    /**
     * Return a {@link VectorOfBooleans} from a vector of booleans
     * 
     * @param vec the vector of booleans
     * @return the vector
     */

    VectorOfBooleans vectorOf( final boolean[] vec );

    /**
     * Return a {@link VectorOfBooleans} from a vector of booleans
     * 
     * @param vec the vector of booleans
     * @return the vector
     */

    MatrixOfDoubles matrixOf( final double[][] vec );

    /**
     * Return a {@link DoubleScoreOutput} with a single component. The score component is stored against the 
     * {@link MetricOutputMetadata#getMetricComponentID()} if this is defined, otherwise {@link MetricConstants#MAIN}.
     * 
     * @param output the output data
     * @param meta the metadata
     * @return a {@link DoubleScoreOutput}
     */

    DoubleScoreOutput ofDoubleScoreOutput( final double output, final MetricOutputMetadata meta );

    /**
     * Return a {@link DoubleScoreOutput} with multiple components.
     * 
     * @param output the output data
     * @param meta the metadata
     * @return a {@link DoubleScoreOutput}
     */

    DoubleScoreOutput ofDoubleScoreOutput( final Map<MetricConstants, Double> output, final MetricOutputMetadata meta );

    /**
     * Return a {@link DoubleScoreOutput} with multiple components that are ordered according to the template provided.
     * 
     * @param output the output data
     * @param template the template
     * @param meta the metadata
     * @return a {@link DoubleScoreOutput}
     */

    DoubleScoreOutput ofDoubleScoreOutput( final double[] output,
                                           ScoreOutputGroup template,
                                           final MetricOutputMetadata meta );

    /**
     * Return a {@link MultiVectorOutput}.
     * 
     * @param output the output data
     * @param meta the metadata
     * @return a {@link MultiVectorOutput}
     */

    MultiVectorOutput ofMultiVectorOutput( final Map<MetricDimension, double[]> output,
                                           final MetricOutputMetadata meta );

    /**
     * Return a {@link MatrixOutput}.
     * 
     * @param output the output data
     * @param meta the metadata
     * @return a {@link MatrixOutput}
     */

    MatrixOutput ofMatrixOutput( final double[][] output, final MetricOutputMetadata meta );

    /**
     * Return a {@link BoxPlotOutput}.
     * 
     * @param output the box plot data
     * @param probabilities the probabilities
     * @param meta the box plot metadata
     * @param domainAxisDimension the domain axis dimension
     * @param rangeAxisDimension the range axis dimension
     * @return a container for box plots
     * @throws MetricOutputException if any of the inputs are invalid
     */

    BoxPlotOutput ofBoxPlotOutput( List<PairOfDoubleAndVectorOfDoubles> output,
                                   VectorOfDoubles probabilities,
                                   MetricOutputMetadata meta,
                                   MetricDimension domainAxisDimension,
                                   MetricDimension rangeAxisDimension );

    /**
     * Return a {@link PairedOutput} that contains a list of pairs.
     * 
     * @param <S> the type for the left of the pair
     * @param <T> the type for the right of the pair
     * @param output the output
     * @param meta the output metadata
     * @return a paired output
     * @throws MetricOutputException if any of the inputs are invalid
     */

    <S, T> PairedOutput<S, T> ofPairedOutput( List<Pair<S, T>> output,
                                              MetricOutputMetadata meta );

    /**
     * Return a {@link DurationScoreOutput} with a single component. The score component is stored against the 
     * {@link MetricOutputMetadata#getMetricComponentID()} if this is defined, otherwise {@link MetricConstants#MAIN}.
     * 
     * @param output the output data
     * @param meta the metadata
     * @return a {@link DurationScoreOutput}
     */

    DurationScoreOutput ofDurationScoreOutput( final Duration output, final MetricOutputMetadata meta );

    /**
     * Return a {@link DurationScoreOutput} with multiple components.
     * 
     * @param output the output data
     * @param meta the metadata
     * @return a {@link DurationScoreOutput}
     */

    DurationScoreOutput ofDurationScoreOutput( final Map<MetricConstants, Duration> output,
                                             final MetricOutputMetadata meta );

    /**
     * Returns a {@link MapKey} to map a {@link MetricOutput} by an elementary key.
     * 
     * @param <S> the type of key
     * @param key the key
     * @return a map key
     */

    <S extends Comparable<S>> MapKey<S> getMapKey( S key );


    /**
     * Returns {@link Threshold} from the specified input.
     * 
     * @param threshold the threshold value or lower bound of a {@link Operator#BETWEEN} condition
     * @param thresholdUpper the upper threshold of a {@link Operator#BETWEEN} or null
     * @param condition the threshold condition
     * @return a threshold
     */

    Threshold getThreshold( final Double threshold, final Double thresholdUpper, final Operator condition );

    /**
     * Returns {@link Threshold} from the specified input. Both inputs must be in the unit interval, [0,1].
     * 
     * @param threshold the threshold value or lower bound of a {@link Operator#BETWEEN} condition
     * @param thresholdUpper the upper threshold of a {@link Operator#BETWEEN} or null
     * @param condition the threshold condition
     * @return a threshold
     */

    Threshold getProbabilityThreshold( final Double threshold,
                                       final Double thresholdUpper,
                                       final Operator condition );

    /**
     * Returns a {@link Threshold} from the specified input
     * 
     * @param threshold the threshold value or lower bound of a {@link Operator#BETWEEN} condition
     * @param thresholdUpper the upper threshold of a {@link Operator#BETWEEN} or null
     * @param probability the probability associated with the threshold
     * @param probabilityUpper the probability associated with the upper threshold or null
     * @param condition the threshold condition
     * @return a quantile
     */

    Threshold getQuantileThreshold( final Double threshold,
                                    final Double thresholdUpper,
                                    Double probability,
                                    Double probabilityUpper,
                                    final Operator condition );

    /**
     * Returns a {@link MetricOutputMapByTimeAndThreshold} from the raw map of inputs.
     * 
     * @param <T> the type of output
     * @param input the map of metric outputs
     * @return a {@link MetricOutputMapByTimeAndThreshold} of metric outputs
     */

    <T extends MetricOutput<?>> MetricOutputMapByTimeAndThreshold<T>
            ofMap( final Map<Pair<TimeWindow, Threshold>, T> input );

    /**
     * Returns a {@link MetricOutputMultiMapByTimeAndThreshold} from a map of inputs by {@link TimeWindow} and 
     * {@link Threshold}.
     * 
     * @param <T> the type of output
     * @param input the input map of metric outputs by time window and threshold
     * @return a map of metric outputs by lead time and threshold for several metrics
     * @throws MetricOutputException if attempting to add multiple results for the same metric by time and threshold
     */

    <T extends MetricOutput<?>> MetricOutputMultiMapByTimeAndThreshold<T>
            ofMultiMap( final Map<Pair<TimeWindow, Threshold>, List<MetricOutputMapByMetric<T>>> input );

    /**
     * Returns a builder for a {@link MetricOutputMultiMapByTimeAndThreshold} that allows for the incremental addition of
     * {@link MetricOutputMapByTimeAndThreshold} as they are computed.
     * 
     * @param <T> the type of output
     * @return a {@link MetricOutputMultiMapByTimeAndThresholdBuilder} for a map of metric outputs by time window and
     *         threshold
     */

    <T extends MetricOutput<?>> MetricOutputMultiMapByTimeAndThresholdBuilder<T> ofMultiMap();

    /**
     * Returns a builder for a {@link MetricOutputForProjectByTimeAndThreshold}.
     * 
     * @return a {@link MetricOutputForProjectByTimeAndThresholdBuilder} for a map of metric outputs by time window and
     *         threshold
     */

    MetricOutputForProjectByTimeAndThresholdBuilder ofMetricOutputForProjectByTimeAndThreshold();

    /**
     * Returns a builder for a {@link TimeSeriesOfSingleValuedPairs} with a regular timestep.
     * 
     * @return a {@link RegularTimeSeriesOfSingleValuedPairsBuilder}
     */

    RegularTimeSeriesOfSingleValuedPairsBuilder ofRegularTimeSeriesOfSingleValuedPairsBuilder();

    /**
     * Returns a builder for a {@link TimeSeriesOfEnsemblePairs} with a regular timestep.
     * 
     * @return a {@link RegularTimeSeriesOfEnsemblePairsBuilder}
     */

    RegularTimeSeriesOfEnsemblePairsBuilder ofRegularTimeSeriesOfEnsemblePairsBuilder();

    /**
     * Returns a {@link MetricOutputMapByMetric} from the raw list of inputs.
     * 
     * @param <T> the type of output
     * @param input the list of metric outputs
     * @return a {@link MetricOutputMapByMetric} of metric outputs
     */

    <T extends MetricOutput<?>> MetricOutputMapByMetric<T> ofMap( final List<T> input );

    /**
     * Combines a list of {@link MetricOutputMapByTimeAndThreshold} into a single map.
     * 
     * @param <T> the type of output
     * @param input the list of input maps
     * @return a combined {@link MetricOutputMapByTimeAndThreshold} of metric outputs
     */

    <T extends MetricOutput<?>> MetricOutputMapByTimeAndThreshold<T>
            combine( final List<MetricOutputMapByTimeAndThreshold<T>> input );

    /**
     * Helper that checks for the equality of two double values using a prescribed number of significant digits.
     * 
     * @param first the first double
     * @param second the second double
     * @param digits the number of significant digits
     * @return true if the first and second are equal to the number of significant digits
     */

    boolean doubleEquals( double first, double second, int digits );

}
