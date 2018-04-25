package wres.datamodel;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.MetricConstants.ScoreOutputGroup;
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
import wres.datamodel.inputs.pairs.TimeSeriesOfEnsemblePairsBuilder;
import wres.datamodel.inputs.pairs.TimeSeriesOfSingleValuedPairs;
import wres.datamodel.inputs.pairs.TimeSeriesOfSingleValuedPairsBuilder;
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
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.Threshold;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;
import wres.datamodel.thresholds.ThresholdsByMetric;
import wres.datamodel.thresholds.ThresholdsByMetric.ThresholdsByMetricBuilder;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesBuilder;

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
     * Returns an instance of {@link OneOrTwoDoubles}.
     * 
     * @param first the first value
     * @return a composition of doubles
     */

    default OneOrTwoDoubles ofOneOrTwoDoubles( Double first )
    {
        return this.ofOneOrTwoDoubles( first, null );
    }

    /**
     * Convenience method that returns a {@link Pair} to map a {@link MetricOutput} by {@link TimeWindow} and
     * {@link OneOrTwoThresholds}.
     * 
     * @param timeWindow the time window
     * @param values the values
     * @param condition the threshold condition
     * @param dataType the data to which the threshold applies
     * @return a map key
     */

    default Pair<TimeWindow, OneOrTwoThresholds> ofMapKeyByTimeThreshold( TimeWindow timeWindow,
                                                                          OneOrTwoDoubles values,
                                                                          Operator condition,
                                                                          ThresholdDataType dataType )
    {
        return Pair.of( timeWindow, OneOrTwoThresholds.of( this.ofThreshold( values, condition, dataType ) ) );
    }

    /**
     * Returns {@link Threshold} from the specified input.
     * 
     * @param values the values
     * @param condition the threshold condition
     * @param dataType the data to which the threshold applies
     * @return a threshold
     */

    default Threshold ofThreshold( OneOrTwoDoubles values, Operator condition, ThresholdDataType dataType )
    {
        return this.ofThreshold( values, condition, dataType, null, null );
    }

    /**
     * Returns {@link Threshold} from the specified input.
     * 
     * @param values the values
     * @param condition the threshold condition
     * @param units the optional units for the threshold values
     * @param dataType the data to which the threshold applies
     * @return a threshold
     */

    default Threshold ofThreshold( OneOrTwoDoubles values,
                                   Operator condition,
                                   ThresholdDataType dataType,
                                   Dimension units )
    {
        return this.ofThreshold( values, condition, dataType, null, units );
    }

    /**
     * Returns {@link Threshold} from the specified input.
     * 
     * @param values the values
     * @param condition the threshold condition
     * @param label an optional label
     * @param dataType the data to which the threshold applies
     * @return a threshold
     */

    default Threshold ofThreshold( OneOrTwoDoubles values,
                                   Operator condition,
                                   ThresholdDataType dataType,
                                   String label )
    {
        return this.ofThreshold( values, condition, dataType, label, null );
    }

    /**
     * Returns {@link Threshold} from the specified input.
     * 
     * @param probabilities the probabilities
     * @param condition the threshold condition
     * @param dataType the data to which the threshold applies
     * @return a threshold
     */

    default Threshold ofProbabilityThreshold( OneOrTwoDoubles probabilities,
                                              Operator condition,
                                              ThresholdDataType dataType )
    {
        return this.ofProbabilityThreshold( probabilities, condition, dataType, null, null );
    }

    /**
     * Returns {@link Threshold} from the specified input.
     * 
     * @param probabilities the probabilities
     * @param condition the threshold condition
     * @param dataType the data to which the threshold applies
     * @param units the optional units for the threshold values
     * @return a threshold
     */

    default Threshold ofProbabilityThreshold( OneOrTwoDoubles probabilities,
                                              Operator condition,
                                              ThresholdDataType dataType,
                                              Dimension units )
    {
        return this.ofProbabilityThreshold( probabilities, condition, dataType, null, units );
    }

    /**
     * Returns {@link Threshold} from the specified input.
     * 
     * @param probabilities the probabilities
     * @param condition the threshold condition
     * @param dataType the data to which the threshold applies
     * @param label an optional label
     * @return a threshold
     */

    default Threshold ofProbabilityThreshold( OneOrTwoDoubles probabilities,
                                              Operator condition,
                                              ThresholdDataType dataType,
                                              String label )
    {
        return this.ofProbabilityThreshold( probabilities, condition, dataType, label, null );
    }

    /**
     * Returns {@link Threshold} from the specified input.
     * 
     * @param values the values
     * @param probabilities the probabilities
     * @param condition the threshold condition
     * @param dataType the data to which the threshold applies
     * @return a threshold
     */

    default Threshold ofQuantileThreshold( OneOrTwoDoubles values,
                                           OneOrTwoDoubles probabilities,
                                           Operator condition,
                                           ThresholdDataType dataType )
    {
        return this.ofQuantileThreshold( values, probabilities, condition, dataType, null, null );
    }

    /**
     * Returns {@link Threshold} from the specified input.
     * 
     * @param values the values
     * @param probabilities the probabilities
     * @param condition the threshold condition
     * @param dataType the data to which the threshold applies
     * @param units the optional units for the threshold values
     * @return a threshold
     */

    default Threshold ofQuantileThreshold( OneOrTwoDoubles values,
                                           OneOrTwoDoubles probabilities,
                                           Operator condition,
                                           ThresholdDataType dataType,
                                           Dimension units )
    {
        return this.ofQuantileThreshold( values, probabilities, condition, dataType, null, units );
    }

    /**
     * Returns {@link Threshold} from the specified input.
     * 
     * @param values the values
     * @param probabilities the probabilities
     * @param condition the threshold condition
     * @param dataType the data to which the threshold applies
     * @param label an optional label
     * @return a threshold
     */

    default Threshold ofQuantileThreshold( OneOrTwoDoubles values,
                                           OneOrTwoDoubles probabilities,
                                           Operator condition,
                                           ThresholdDataType dataType,
                                           String label )
    {
        return this.ofQuantileThreshold( values, probabilities, condition, dataType, label, null );
    }

    /**
     * Return a {@link MatrixOutput}.
     * 
     * @param output the output data
     * @param meta the metadata
     * @return a {@link MatrixOutput}
     */

    default MatrixOutput ofMatrixOutput( double[][] output, MetricOutputMetadata meta )
    {
        return ofMatrixOutput( output, null, meta );
    }

    /**
     * Construct the dichotomous input without any pairs for a baseline.
     * 
     * @param pairs the verification pairs
     * @param meta the metadata
     * @return the pairs
     * @throws MetricInputException if the inputs are invalid
     */

    default DichotomousPairs ofDichotomousPairs( List<VectorOfBooleans> pairs, Metadata meta )
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

    default DichotomousPairs ofDichotomousPairsFromAtomic( List<PairOfBooleans> pairs, Metadata meta )
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

    default MulticategoryPairs ofMulticategoryPairs( List<VectorOfBooleans> pairs, Metadata meta )
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

    default DiscreteProbabilityPairs ofDiscreteProbabilityPairs( List<PairOfDoubles> pairs, Metadata meta )
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

    default SingleValuedPairs ofSingleValuedPairs( List<PairOfDoubles> pairs, Metadata meta )
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

    default EnsemblePairs ofEnsemblePairs( List<PairOfDoubleAndVectorOfDoubles> pairs, Metadata meta )
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

    default DichotomousPairs ofDichotomousPairs( List<VectorOfBooleans> pairs,
                                                 Metadata meta,
                                                 VectorOfDoubles climatology )
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

    default DichotomousPairs ofDichotomousPairsFromAtomic( List<PairOfBooleans> pairs,
                                                           Metadata meta,
                                                           VectorOfDoubles climatology )
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

    default MulticategoryPairs ofMulticategoryPairs( List<VectorOfBooleans> pairs,
                                                     Metadata meta,
                                                     VectorOfDoubles climatology )
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

    default DiscreteProbabilityPairs ofDiscreteProbabilityPairs( List<PairOfDoubles> pairs,
                                                                 Metadata meta,
                                                                 VectorOfDoubles climatology )
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

    default SingleValuedPairs ofSingleValuedPairs( List<PairOfDoubles> pairs,
                                                   Metadata meta,
                                                   VectorOfDoubles climatology )
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

    default EnsemblePairs ofEnsemblePairs( List<PairOfDoubleAndVectorOfDoubles> pairs,
                                           Metadata meta,
                                           VectorOfDoubles climatology )
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

    default SingleValuedPairs ofSingleValuedPairs( List<PairOfDoubles> pairs,
                                                   List<PairOfDoubles> basePairs,
                                                   Metadata mainMeta,
                                                   Metadata baselineMeta )
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

    default EnsemblePairs ofEnsemblePairs( List<PairOfDoubleAndVectorOfDoubles> pairs,
                                           List<PairOfDoubleAndVectorOfDoubles> basePairs,
                                           Metadata mainMeta,
                                           Metadata baselineMeta )
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

    default MulticategoryPairs ofMulticategoryPairs( List<VectorOfBooleans> pairs,
                                                     List<VectorOfBooleans> basePairs,
                                                     Metadata mainMeta,
                                                     Metadata baselineMeta )
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

    default DiscreteProbabilityPairs ofDiscreteProbabilityPairs( List<PairOfDoubles> pairs,
                                                                 List<PairOfDoubles> basePairs,
                                                                 Metadata mainMeta,
                                                                 Metadata baselineMeta )
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

    default DichotomousPairs ofDichotomousPairs( List<VectorOfBooleans> pairs,
                                                 List<VectorOfBooleans> basePairs,
                                                 Metadata mainMeta,
                                                 Metadata baselineMeta )
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
     * Forms the union of the {@link PairedOutput}, returning a {@link PairedOutput} that contains all of the pairs in 
     * the inputs.
     * 
     * @param <S> the left side of the paired output
     * @param <T> the right side of the paired output
     * @param collection the list of inputs
     * @return a combined {@link PairedOutput}
     * @throws NullPointerException if the input is null
     */

    default <S, T> PairedOutput<S, T> unionOf( Collection<PairedOutput<S, T>> collection )
    {
        Objects.requireNonNull( collection );
        List<Pair<S, T>> combined = new ArrayList<>();
        List<TimeWindow> combinedWindows = new ArrayList<>();
        MetricOutputMetadata sourceMeta = null;
        for ( PairedOutput<S, T> next : collection )
        {
            combined.addAll( next.getData() );
            if ( Objects.isNull( sourceMeta ) )
            {
                sourceMeta = next.getMetadata();
            }
            combinedWindows.add( next.getMetadata().getTimeWindow() );
        }
        TimeWindow unionWindow = null;
        if ( !combinedWindows.isEmpty() )
        {
            unionWindow = TimeWindow.unionOf( combinedWindows );
        }

        MetadataFactory mDF = getMetadataFactory();
        MetricOutputMetadata combinedMeta =
                mDF.getOutputMetadata( mDF.getOutputMetadata( sourceMeta, combined.size() ), unionWindow );
        return ofPairedOutput( combined, combinedMeta );
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
     * Returns an instance of {@link OneOrTwoDoubles}.
     * 
     * @param first the first value, which is required
     * @param second the second value, which is optional
     * @return a composition of doubles
     */

    OneOrTwoDoubles ofOneOrTwoDoubles( Double first, Double second );

    /**
     * Returns {@link Threshold} from the specified input.
     * 
     * @param values the threshold values
     * @param condition the threshold condition
     * @param dataType the data to which the threshold applies
     * @param label an optional label
     * @param units the optional units for the threshold values
     * @return a threshold
     */

    Threshold ofThreshold( OneOrTwoDoubles values,
                           Operator condition,
                           ThresholdDataType dataType,
                           String label,
                           Dimension units );

    /**
     * Returns {@link Threshold} from the specified input. Both inputs must be in the unit interval, [0,1].
     * 
     * @param probabilities the probabilities
     * @param condition the threshold condition
     * @param dataType the data to which the threshold applies
     * @param label an optional label
     * @param units an optional set of units to use when deriving quantiles from probability thresholds
     * @return a threshold
     */

    Threshold ofProbabilityThreshold( OneOrTwoDoubles probabilities,
                                      Operator condition,
                                      ThresholdDataType dataType,
                                      String label,
                                      Dimension units );

    /**
     * Returns a {@link Threshold} from the specified input
     * 
     * @param values the value or null
     * @param probabilities the probabilities or null
     * @param condition the threshold condition
     * @param dataType the data to which the threshold applies
     * @param label an optional label
     * @param units the optional units for the quantiles
     * @return a quantile
     */

    Threshold ofQuantileThreshold( OneOrTwoDoubles values,
                                   OneOrTwoDoubles probabilities,
                                   Operator condition,
                                   ThresholdDataType dataType,
                                   String label,
                                   Dimension units );

    /**
     * Returns a builder for a {@link ThresholdsByMetric}.
     * 
     * @return a builder
     */

    ThresholdsByMetricBuilder ofThresholdsByMetricBuilder();

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

    SingleValuedPairs ofSingleValuedPairs( List<PairOfDoubles> pairs,
                                           List<PairOfDoubles> basePairs,
                                           Metadata mainMeta,
                                           Metadata baselineMeta,
                                           VectorOfDoubles climatology );

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

    EnsemblePairs ofEnsemblePairs( List<PairOfDoubleAndVectorOfDoubles> pairs,
                                   List<PairOfDoubleAndVectorOfDoubles> basePairs,
                                   Metadata mainMeta,
                                   Metadata baselineMeta,
                                   VectorOfDoubles climatology );

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

    MulticategoryPairs ofMulticategoryPairs( List<VectorOfBooleans> pairs,
                                             List<VectorOfBooleans> basePairs,
                                             Metadata mainMeta,
                                             Metadata baselineMeta,
                                             VectorOfDoubles climatology );

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

    DiscreteProbabilityPairs ofDiscreteProbabilityPairs( List<PairOfDoubles> pairs,
                                                         List<PairOfDoubles> basePairs,
                                                         Metadata mainMeta,
                                                         Metadata baselineMeta,
                                                         VectorOfDoubles climatology );

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

    DichotomousPairs ofDichotomousPairs( List<VectorOfBooleans> pairs,
                                         List<VectorOfBooleans> basePairs,
                                         Metadata mainMeta,
                                         Metadata baselineMeta,
                                         VectorOfDoubles climatology );

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

    DichotomousPairs ofDichotomousPairsFromAtomic( List<PairOfBooleans> pairs,
                                                   List<PairOfBooleans> basePairs,
                                                   Metadata mainMeta,
                                                   Metadata baselineMeta,
                                                   VectorOfDoubles climatology );

    /**
     * Return a {@link PairOfDoubles} from two double values.
     * 
     * @param left the left value
     * @param right the right value
     * @return the pair
     */

    PairOfDoubles pairOf( double left, double right );

    /**
     * Return a {@link PairOfBooleans} from two boolean values.
     * 
     * @param left the first value
     * @param right the second value
     * @return the pair
     */

    PairOfBooleans pairOf( boolean left, boolean right );

    /**
     * Return a {@link PairOfDoubleAndVectorOfDoubles} from a double value and a double vector of values.
     * 
     * @param left the first value
     * @param right the second value
     * @return the pair
     */

    PairOfDoubleAndVectorOfDoubles pairOf( double left, double[] right );

    /**
     * Return a {@link PairOfDoubleAndVectorOfDoubles} from a double value and a double vector of values.
     * 
     * @param left the first value
     * @param right the second value
     * @return the pair
     */

    PairOfDoubleAndVectorOfDoubles pairOf( Double left, Double[] right );

    /**
     * Return a {@link Pair} from two double vectors.
     * 
     * @param left the first value
     * @param right the second value
     * @return the pair
     */

    Pair<VectorOfDoubles, VectorOfDoubles> pairOf( double[] left, double[] right );

    /**
     * Return a {@link VectorOfDoubles} from a vector of doubles
     * 
     * @param vec the vector of doubles
     * @return the vector
     */

    VectorOfDoubles vectorOf( double[] vec );

    /**
     * Return a {@link VectorOfDoubles} from a vector of doubles
     * 
     * @param vec the vector of doubles
     * @return the vector
     */

    VectorOfDoubles vectorOf( Double[] vec );

    /**
     * Return a {@link VectorOfBooleans} from a vector of booleans
     * 
     * @param vec the vector of booleans
     * @return the vector
     */

    VectorOfBooleans vectorOf( boolean[] vec );

    /**
     * Return a {@link VectorOfBooleans} from a vector of booleans
     * 
     * @param vec the vector of booleans
     * @return the vector
     */

    MatrixOfDoubles matrixOf( double[][] vec );

    /**
     * Return a {@link DoubleScoreOutput} with a single component. The score component is stored against the 
     * {@link MetricOutputMetadata#getMetricComponentID()} if this is defined, otherwise {@link MetricConstants#MAIN}.
     * 
     * @param output the output data
     * @param meta the metadata
     * @return a {@link DoubleScoreOutput}
     */

    DoubleScoreOutput ofDoubleScoreOutput( double output, MetricOutputMetadata meta );

    /**
     * Return a {@link DoubleScoreOutput} with multiple components.
     * 
     * @param output the output data
     * @param meta the metadata
     * @return a {@link DoubleScoreOutput}
     */

    DoubleScoreOutput ofDoubleScoreOutput( Map<MetricConstants, Double> output, MetricOutputMetadata meta );

    /**
     * Return a {@link DoubleScoreOutput} with multiple components that are ordered according to the template provided.
     * 
     * @param output the output data
     * @param template the template
     * @param meta the metadata
     * @return a {@link DoubleScoreOutput}
     */

    DoubleScoreOutput ofDoubleScoreOutput( double[] output,
                                           ScoreOutputGroup template,
                                           MetricOutputMetadata meta );

    /**
     * Return a {@link MultiVectorOutput}.
     * 
     * @param output the output data
     * @param meta the metadata
     * @return a {@link MultiVectorOutput}
     */

    MultiVectorOutput ofMultiVectorOutput( Map<MetricDimension, double[]> output,
                                           MetricOutputMetadata meta );

    /**
     * Return a {@link MatrixOutput}.
     * 
     * @param output the output data
     * @param names an optional list of names in row-major order, with as many elements as the cardinality of the matrix
     * @param meta the metadata
     * @return a {@link MatrixOutput}
     */

    MatrixOutput ofMatrixOutput( double[][] output,
                                 List<MetricDimension> names,
                                 MetricOutputMetadata meta );

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

    DurationScoreOutput ofDurationScoreOutput( Duration output, MetricOutputMetadata meta );

    /**
     * Return a {@link DurationScoreOutput} with multiple components.
     * 
     * @param output the output data
     * @param meta the metadata
     * @return a {@link DurationScoreOutput}
     */

    DurationScoreOutput ofDurationScoreOutput( Map<MetricConstants, Duration> output,
                                               MetricOutputMetadata meta );

    /**
     * Returns a {@link MapKey} to map a {@link MetricOutput} by an elementary key.
     * 
     * @param <S> the type of key
     * @param key the key
     * @return a map key
     */

    <S extends Comparable<S>> MapKey<S> getMapKey( S key );

    /**
     * Returns a {@link MetricOutputMapByTimeAndThreshold} from the raw map of inputs.
     * 
     * @param <T> the type of output
     * @param input the map of metric outputs
     * @return a {@link MetricOutputMapByTimeAndThreshold} of metric outputs
     */

    <T extends MetricOutput<?>> MetricOutputMapByTimeAndThreshold<T>
            ofMetricOutputMapByTimeAndThreshold( Map<Pair<TimeWindow, OneOrTwoThresholds>, T> input );

    /**
     * Returns a {@link MetricOutputMultiMapByTimeAndThreshold} from a map of inputs by {@link TimeWindow} and 
     * {@link OneOrTwoThresholds}.
     * 
     * @param <T> the type of output
     * @param input the input map of metric outputs by time window and threshold
     * @return a map of metric outputs by lead time and threshold for several metrics
     * @throws MetricOutputException if attempting to add multiple results for the same metric by time and threshold
     */

    <T extends MetricOutput<?>> MetricOutputMultiMapByTimeAndThreshold<T>
            ofMetricOutputMultiMapByTimeAndThreshold( Map<Pair<TimeWindow, OneOrTwoThresholds>, List<MetricOutputMapByMetric<T>>> input );

    /**
     * Returns a builder for a {@link MetricOutputMultiMapByTimeAndThreshold} that allows for the incremental addition of
     * {@link MetricOutputMapByTimeAndThreshold} as they are computed.
     * 
     * @param <T> the type of output
     * @return a {@link MetricOutputMultiMapByTimeAndThresholdBuilder} for a map of metric outputs by time window and
     *         threshold
     */

    <T extends MetricOutput<?>> MetricOutputMultiMapByTimeAndThresholdBuilder<T>
            ofMetricOutputMultiMapByTimeAndThresholdBuilder();

    /**
     * Returns a builder for a {@link MetricOutputForProjectByTimeAndThreshold}.
     * 
     * @return a {@link MetricOutputForProjectByTimeAndThresholdBuilder} for a map of metric outputs by time window and
     *         threshold
     */

    MetricOutputForProjectByTimeAndThresholdBuilder ofMetricOutputForProjectByTimeAndThreshold();

    /**
     * Returns a builder for a {@link TimeSeries} whose timestep may vary.
     * 
     * @param <T> the type of data
     * @return a {@link TimeSeriesBuilder}
     */

    <T> TimeSeriesBuilder<T> ofTimeSeriesBuilder();
    
    /**
     * Returns a builder for a {@link TimeSeriesOfSingleValuedPairs} whose timestep may vary.
     * 
     * @return a {@link TimeSeriesOfSingleValuedPairsBuilder}
     */

    TimeSeriesOfSingleValuedPairsBuilder ofTimeSeriesOfSingleValuedPairsBuilder();

    /**
     * Returns a builder for a {@link TimeSeriesOfEnsemblePairs} whose timestep may vary.
     * 
     * @return a {@link TimeSeriesOfEnsemblePairsBuilder}
     */

    TimeSeriesOfEnsemblePairsBuilder ofTimeSeriesOfEnsemblePairsBuilder();

    /**
     * Returns a {@link MetricOutputMapByMetric} from the raw map of inputs.
     * 
     * @param <T> the type of output
     * @param input the map of metric outputs
     * @return a {@link MetricOutputMapByMetric} of metric outputs
     */

    <T extends MetricOutput<?>> MetricOutputMapByMetric<T> ofMetricOutputMapByMetric( Map<MetricConstants, T> input );

    /**
     * Combines a list of {@link MetricOutputMapByTimeAndThreshold} into a single map.
     * 
     * @param <T> the type of output
     * @param input the list of input maps
     * @return a combined {@link MetricOutputMapByTimeAndThreshold} of metric outputs
     */

    <T extends MetricOutput<?>> MetricOutputMapByTimeAndThreshold<T>
            combine( List<MetricOutputMapByTimeAndThreshold<T>> input );

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
