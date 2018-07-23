package wres.datamodel;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
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
import wres.datamodel.inputs.pairs.SafeDichotomousPairs;
import wres.datamodel.inputs.pairs.SafeDiscreteProbabilityPairs;
import wres.datamodel.inputs.pairs.SafeEnsemblePairs;
import wres.datamodel.inputs.pairs.SafeMulticategoryPairs;
import wres.datamodel.inputs.pairs.SafePairOfDoubleAndVectorOfDoubles;
import wres.datamodel.inputs.pairs.SafePairOfDoubles;
import wres.datamodel.inputs.pairs.SafeSingleValuedPairs;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.inputs.pairs.TimeSeriesOfEnsemblePairs;
import wres.datamodel.inputs.pairs.TimeSeriesOfEnsemblePairsBuilder;
import wres.datamodel.inputs.pairs.TimeSeriesOfSingleValuedPairs;
import wres.datamodel.inputs.pairs.TimeSeriesOfSingleValuedPairsBuilder;
import wres.datamodel.metadata.Dimension;
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
import wres.datamodel.outputs.SafeBoxPlotOutput;
import wres.datamodel.outputs.SafeDoubleScoreOutput;
import wres.datamodel.outputs.SafeDurationScoreOutput;
import wres.datamodel.outputs.SafeMatrixOutput;
import wres.datamodel.outputs.SafeMetricOutputForProjectByTimeAndThreshold;
import wres.datamodel.outputs.SafeMetricOutputMapByMetric.SafeMetricOutputMapByMetricBuilder;
import wres.datamodel.outputs.SafeMetricOutputMapByTimeAndThreshold;
import wres.datamodel.outputs.SafeMetricOutputMapByTimeAndThreshold.SafeMetricOutputMapByTimeAndThresholdBuilder;
import wres.datamodel.outputs.SafeMetricOutputMultiMapByTimeAndThreshold.SafeMetricOutputMultiMapByTimeAndThresholdBuilder;
import wres.datamodel.outputs.SafeMultiVectorOutput;
import wres.datamodel.outputs.SafePairedOutput;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.SafeThreshold;
import wres.datamodel.thresholds.SafeThresholdsByMetric.SafeThresholdsByMetricBuilder;
import wres.datamodel.thresholds.Threshold;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;
import wres.datamodel.thresholds.ThresholdsByMetric;
import wres.datamodel.thresholds.ThresholdsByMetric.ThresholdsByMetricBuilder;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesBuilder;
import wres.datamodel.time.SafeTimeSeries.SafeTimeSeriesBuilder;
import wres.datamodel.inputs.pairs.SafeTimeSeriesOfSingleValuedPairs.SafeTimeSeriesOfSingleValuedPairsBuilder;
import wres.datamodel.inputs.pairs.SafeTimeSeriesOfEnsemblePairs.SafeTimeSeriesOfEnsemblePairsBuilder;;


/**
 * A factory class for producing datasets associated with verification metrics.
 * 
 * @author james.brown@hydrosolved.com
 */

public final class DataFactory
{

    /**
     * Returns an instance of {@link OneOrTwoDoubles}.
     * 
     * @param first the first value
     * @return a composition of doubles
     */

    public static OneOrTwoDoubles ofOneOrTwoDoubles( Double first )
    {
        return DataFactory.ofOneOrTwoDoubles( first, null );
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

    public static Pair<TimeWindow, OneOrTwoThresholds> ofMapKeyByTimeThreshold( TimeWindow timeWindow,
                                                                                OneOrTwoDoubles values,
                                                                                Operator condition,
                                                                                ThresholdDataType dataType )
    {
        return Pair.of( timeWindow, OneOrTwoThresholds.of( DataFactory.ofThreshold( values, condition, dataType ) ) );
    }

    /**
     * Returns {@link Threshold} from the specified input.
     * 
     * @param values the values
     * @param condition the threshold condition
     * @param dataType the data to which the threshold applies
     * @return a threshold
     */

    public static Threshold ofThreshold( OneOrTwoDoubles values, Operator condition, ThresholdDataType dataType )
    {
        return DataFactory.ofThreshold( values, condition, dataType, null, null );
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

    public static Threshold ofThreshold( OneOrTwoDoubles values,
                                         Operator condition,
                                         ThresholdDataType dataType,
                                         Dimension units )
    {
        return DataFactory.ofThreshold( values, condition, dataType, null, units );
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

    public static Threshold ofThreshold( OneOrTwoDoubles values,
                                         Operator condition,
                                         ThresholdDataType dataType,
                                         String label )
    {
        return DataFactory.ofThreshold( values, condition, dataType, label, null );
    }

    /**
     * Returns {@link Threshold} from the specified input.
     * 
     * @param probabilities the probabilities
     * @param condition the threshold condition
     * @param dataType the data to which the threshold applies
     * @return a threshold
     */

    public static Threshold ofProbabilityThreshold( OneOrTwoDoubles probabilities,
                                                    Operator condition,
                                                    ThresholdDataType dataType )
    {
        return DataFactory.ofProbabilityThreshold( probabilities, condition, dataType, null, null );
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

    public static Threshold ofProbabilityThreshold( OneOrTwoDoubles probabilities,
                                                    Operator condition,
                                                    ThresholdDataType dataType,
                                                    Dimension units )
    {
        return DataFactory.ofProbabilityThreshold( probabilities, condition, dataType, null, units );
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

    public static Threshold ofProbabilityThreshold( OneOrTwoDoubles probabilities,
                                                    Operator condition,
                                                    ThresholdDataType dataType,
                                                    String label )
    {
        return DataFactory.ofProbabilityThreshold( probabilities, condition, dataType, label, null );
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

    public static Threshold ofQuantileThreshold( OneOrTwoDoubles values,
                                                 OneOrTwoDoubles probabilities,
                                                 Operator condition,
                                                 ThresholdDataType dataType )
    {
        return DataFactory.ofQuantileThreshold( values, probabilities, condition, dataType, null, null );
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

    public static Threshold ofQuantileThreshold( OneOrTwoDoubles values,
                                                 OneOrTwoDoubles probabilities,
                                                 Operator condition,
                                                 ThresholdDataType dataType,
                                                 Dimension units )
    {
        return DataFactory.ofQuantileThreshold( values, probabilities, condition, dataType, null, units );
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

    public static Threshold ofQuantileThreshold( OneOrTwoDoubles values,
                                                 OneOrTwoDoubles probabilities,
                                                 Operator condition,
                                                 ThresholdDataType dataType,
                                                 String label )
    {
        return DataFactory.ofQuantileThreshold( values, probabilities, condition, dataType, label, null );
    }

    /**
     * Return a {@link MatrixOutput}.
     * 
     * @param output the output data
     * @param meta the metadata
     * @return a {@link MatrixOutput}
     */

    public static MatrixOutput ofMatrixOutput( double[][] output, MetricOutputMetadata meta )
    {
        return DataFactory.ofMatrixOutput( output, null, meta );
    }

    /**
     * Construct the dichotomous input without any pairs for a baseline.
     * 
     * @param pairs the verification pairs
     * @param meta the metadata
     * @return the pairs
     * @throws MetricInputException if the inputs are invalid
     */

    public static DichotomousPairs ofDichotomousPairs( List<VectorOfBooleans> pairs, Metadata meta )
    {
        return DataFactory.ofDichotomousPairs( pairs, null, meta, null, null );
    }

    /**
     * Construct the dichotomous input from atomic {@link PairOfBooleans} without any pairs for a baseline.
     * 
     * @param pairs the verification pairs
     * @param meta the metadata
     * @return the pairs
     * @throws MetricInputException if the inputs are invalid
     */

    public static DichotomousPairs ofDichotomousPairsFromAtomic( List<PairOfBooleans> pairs, Metadata meta )
    {
        return DataFactory.ofDichotomousPairsFromAtomic( pairs, null, meta, null, null );
    }

    /**
     * Construct the multicategory input without any pairs for a baseline.
     * 
     * @param pairs the verification pairs
     * @param meta the metadata
     * @return the pairs
     * @throws MetricInputException if the inputs are invalid
     */

    public static MulticategoryPairs ofMulticategoryPairs( List<VectorOfBooleans> pairs, Metadata meta )
    {
        return DataFactory.ofMulticategoryPairs( pairs, null, meta, null, null );
    }

    /**
     * Construct the discrete probability input without any pairs for a baseline.
     * 
     * @param pairs the discrete probability pairs
     * @param meta the metadata
     * @throws MetricInputException if the inputs are invalid
     * @return the pairs
     */

    public static DiscreteProbabilityPairs ofDiscreteProbabilityPairs( List<PairOfDoubles> pairs, Metadata meta )
    {
        return DataFactory.ofDiscreteProbabilityPairs( pairs, null, meta, null, null );
    }

    /**
     * Construct the single-valued input without any pairs for a baseline.
     * 
     * @param pairs the single-valued pairs
     * @param meta the metadata
     * @return the pairs
     * @throws MetricInputException if the inputs are invalid
     */

    public static SingleValuedPairs ofSingleValuedPairs( List<PairOfDoubles> pairs, Metadata meta )
    {
        return DataFactory.ofSingleValuedPairs( pairs, null, meta, null, null );
    }

    /**
     * Construct the ensemble input without any pairs for a baseline.
     * 
     * @param pairs the ensemble pairs
     * @param meta the metadata
     * @return the pairs
     * @throws MetricInputException if the inputs are invalid
     */

    public static EnsemblePairs ofEnsemblePairs( List<PairOfDoubleAndVectorOfDoubles> pairs, Metadata meta )
    {
        return DataFactory.ofEnsemblePairs( pairs, null, meta, null, null );
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

    public static DichotomousPairs ofDichotomousPairs( List<VectorOfBooleans> pairs,
                                                       Metadata meta,
                                                       VectorOfDoubles climatology )
    {
        return DataFactory.ofDichotomousPairs( pairs, null, meta, null, climatology );
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

    public static DichotomousPairs ofDichotomousPairsFromAtomic( List<PairOfBooleans> pairs,
                                                                 Metadata meta,
                                                                 VectorOfDoubles climatology )
    {
        return DataFactory.ofDichotomousPairsFromAtomic( pairs, null, meta, null, climatology );
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

    public static MulticategoryPairs ofMulticategoryPairs( List<VectorOfBooleans> pairs,
                                                           Metadata meta,
                                                           VectorOfDoubles climatology )
    {
        return DataFactory.ofMulticategoryPairs( pairs, null, meta, null, climatology );
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

    public static DiscreteProbabilityPairs ofDiscreteProbabilityPairs( List<PairOfDoubles> pairs,
                                                                       Metadata meta,
                                                                       VectorOfDoubles climatology )
    {
        return DataFactory.ofDiscreteProbabilityPairs( pairs, null, meta, null, climatology );
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

    public static SingleValuedPairs ofSingleValuedPairs( List<PairOfDoubles> pairs,
                                                         Metadata meta,
                                                         VectorOfDoubles climatology )
    {
        return DataFactory.ofSingleValuedPairs( pairs, null, meta, null, climatology );
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

    public static EnsemblePairs ofEnsemblePairs( List<PairOfDoubleAndVectorOfDoubles> pairs,
                                                 Metadata meta,
                                                 VectorOfDoubles climatology )
    {
        return DataFactory.ofEnsemblePairs( pairs, null, meta, null, climatology );
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

    public static SingleValuedPairs ofSingleValuedPairs( List<PairOfDoubles> pairs,
                                                         List<PairOfDoubles> basePairs,
                                                         Metadata mainMeta,
                                                         Metadata baselineMeta )
    {
        return DataFactory.ofSingleValuedPairs( pairs, basePairs, mainMeta, baselineMeta, null );
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

    public static EnsemblePairs ofEnsemblePairs( List<PairOfDoubleAndVectorOfDoubles> pairs,
                                                 List<PairOfDoubleAndVectorOfDoubles> basePairs,
                                                 Metadata mainMeta,
                                                 Metadata baselineMeta )
    {
        return DataFactory.ofEnsemblePairs( pairs, basePairs, mainMeta, baselineMeta, null );
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

    public static MulticategoryPairs ofMulticategoryPairs( List<VectorOfBooleans> pairs,
                                                           List<VectorOfBooleans> basePairs,
                                                           Metadata mainMeta,
                                                           Metadata baselineMeta )
    {
        return DataFactory.ofMulticategoryPairs( pairs, basePairs, mainMeta, baselineMeta, null );
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

    public static DiscreteProbabilityPairs ofDiscreteProbabilityPairs( List<PairOfDoubles> pairs,
                                                                       List<PairOfDoubles> basePairs,
                                                                       Metadata mainMeta,
                                                                       Metadata baselineMeta )
    {
        return DataFactory.ofDiscreteProbabilityPairs( pairs, basePairs, mainMeta, baselineMeta, null );
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

    public static DichotomousPairs ofDichotomousPairs( List<VectorOfBooleans> pairs,
                                                       List<VectorOfBooleans> basePairs,
                                                       Metadata mainMeta,
                                                       Metadata baselineMeta )
    {
        return DataFactory.ofDichotomousPairs( pairs, basePairs, mainMeta, baselineMeta, null );
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

    public static TimeWindow ofTimeWindow( Instant earliestTime,
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

    public static TimeWindow ofTimeWindow( Instant earliestTime,
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

    public static <S, T> PairedOutput<S, T> unionOf( Collection<PairedOutput<S, T>> collection )
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

        MetricOutputMetadata combinedMeta =
                MetadataFactory.getOutputMetadata( MetadataFactory.getOutputMetadata( sourceMeta,
                                                                                      combined.size() ),
                                                   unionWindow );
        return DataFactory.ofPairedOutput( combined, combinedMeta );
    }

    /**
     * Returns an instance of {@link OneOrTwoDoubles}.
     * 
     * @param first the first value, which is required
     * @param second the second value, which is optional
     * @return a composition of doubles
     */

    public static OneOrTwoDoubles ofOneOrTwoDoubles( Double first, Double second )
    {
        return OneOrTwoDoubles.of( first, second );
    }

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

    public static Threshold ofThreshold( OneOrTwoDoubles values,
                                         Operator condition,
                                         ThresholdDataType dataType,
                                         String label,
                                         Dimension units )
    {
        return new SafeThreshold.ThresholdBuilder().setValues( values )
                                                   .setCondition( condition )
                                                   .setDataType( dataType )
                                                   .setLabel( label )
                                                   .setUnits( units )
                                                   .build();
    }

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

    public static Threshold ofProbabilityThreshold( OneOrTwoDoubles probabilities,
                                                    Operator condition,
                                                    ThresholdDataType dataType,
                                                    String label,
                                                    Dimension units )
    {
        return new SafeThreshold.ThresholdBuilder().setProbabilities( probabilities )
                                                   .setCondition( condition )
                                                   .setDataType( dataType )
                                                   .setLabel( label )
                                                   .setUnits( units )
                                                   .build();
    }

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

    public static Threshold ofQuantileThreshold( OneOrTwoDoubles values,
                                                 OneOrTwoDoubles probabilities,
                                                 Operator condition,
                                                 ThresholdDataType dataType,
                                                 String label,
                                                 Dimension units )
    {
        return new SafeThreshold.ThresholdBuilder().setValues( values )
                                                   .setProbabilities( probabilities )
                                                   .setCondition( condition )
                                                   .setDataType( dataType )
                                                   .setLabel( label )
                                                   .setUnits( units )
                                                   .build();
    }

    /**
     * Returns a builder for a {@link ThresholdsByMetric}.
     * 
     * @return a builder
     */

    public static ThresholdsByMetricBuilder ofThresholdsByMetricBuilder()
    {
        return new SafeThresholdsByMetricBuilder();
    }

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

    public static SingleValuedPairs ofSingleValuedPairs( List<PairOfDoubles> pairs,
                                                         List<PairOfDoubles> basePairs,
                                                         Metadata mainMeta,
                                                         Metadata baselineMeta,
                                                         VectorOfDoubles climatology )
    {
        SafeSingleValuedPairs.SingleValuedPairsBuilder b = new SafeSingleValuedPairs.SingleValuedPairsBuilder();
        return (SingleValuedPairs) b.setMetadata( mainMeta )
                                    .addData( pairs )
                                    .addDataForBaseline( basePairs )
                                    .setMetadataForBaseline( baselineMeta )
                                    .setClimatology( climatology )
                                    .build();
    }

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

    public static EnsemblePairs ofEnsemblePairs( List<PairOfDoubleAndVectorOfDoubles> pairs,
                                                 List<PairOfDoubleAndVectorOfDoubles> basePairs,
                                                 Metadata mainMeta,
                                                 Metadata baselineMeta,
                                                 VectorOfDoubles climatology )
    {
        SafeEnsemblePairs.EnsemblePairsBuilder b = new SafeEnsemblePairs.EnsemblePairsBuilder();
        return (EnsemblePairs) b.setMetadata( mainMeta )
                                .addData( pairs )
                                .addDataForBaseline( basePairs )
                                .setMetadataForBaseline( baselineMeta )
                                .setClimatology( climatology )
                                .build();
    }

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

    public static MulticategoryPairs ofMulticategoryPairs( List<VectorOfBooleans> pairs,
                                                           List<VectorOfBooleans> basePairs,
                                                           Metadata mainMeta,
                                                           Metadata baselineMeta,
                                                           VectorOfDoubles climatology )
    {
        SafeMulticategoryPairs.MulticategoryPairsBuilder b =
                new SafeMulticategoryPairs.MulticategoryPairsBuilder();
        return (MulticategoryPairs) b.addData( pairs )
                                     .setMetadata( mainMeta )
                                     .addDataForBaseline( basePairs )
                                     .setMetadataForBaseline( baselineMeta )
                                     .setClimatology( climatology )
                                     .build();
    }

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

    public static DiscreteProbabilityPairs ofDiscreteProbabilityPairs( List<PairOfDoubles> pairs,
                                                                       List<PairOfDoubles> basePairs,
                                                                       Metadata mainMeta,
                                                                       Metadata baselineMeta,
                                                                       VectorOfDoubles climatology )
    {
        SafeDiscreteProbabilityPairs.DiscreteProbabilityPairsBuilder b =
                new SafeDiscreteProbabilityPairs.DiscreteProbabilityPairsBuilder();
        return (DiscreteProbabilityPairs) b.addData( pairs )
                                           .setMetadata( mainMeta )
                                           .addDataForBaseline( basePairs )
                                           .setMetadataForBaseline( baselineMeta )
                                           .setClimatology( climatology )
                                           .build();
    }

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

    public static DichotomousPairs ofDichotomousPairs( List<VectorOfBooleans> pairs,
                                                       List<VectorOfBooleans> basePairs,
                                                       Metadata mainMeta,
                                                       Metadata baselineMeta,
                                                       VectorOfDoubles climatology )
    {
        SafeDichotomousPairs.DichotomousPairsBuilder b = new SafeDichotomousPairs.DichotomousPairsBuilder();
        return (DichotomousPairs) b.addData( pairs )
                                   .setMetadata( mainMeta )
                                   .addDataForBaseline( basePairs )
                                   .setMetadataForBaseline( baselineMeta )
                                   .setClimatology( climatology )
                                   .build();
    }

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

    public static DichotomousPairs ofDichotomousPairsFromAtomic( List<PairOfBooleans> pairs,
                                                                 List<PairOfBooleans> basePairs,
                                                                 Metadata mainMeta,
                                                                 Metadata baselineMeta,
                                                                 VectorOfDoubles climatology )
    {
        SafeDichotomousPairs.DichotomousPairsBuilder b = new SafeDichotomousPairs.DichotomousPairsBuilder();
        b.setDataFromAtomic( pairs ).setMetadata( mainMeta ).setClimatology( climatology );
        return (DichotomousPairs) b.setDataForBaselineFromAtomic( basePairs )
                                   .setMetadataForBaseline( baselineMeta )
                                   .build();
    }

    /**
     * Return a {@link PairOfDoubles} from two double values.
     * 
     * @param left the left value
     * @param right the right value
     * @return the pair
     */

    public static PairOfDoubles pairOf( double left, double right )
    {
        return new SafePairOfDoubles( left, right );
    }

    /**
     * Return a {@link PairOfBooleans} from two boolean values.
     * 
     * @param left the first value
     * @param right the second value
     * @return the pair
     */

    public static PairOfBooleans pairOf( boolean left, boolean right )
    {
        return new SafePairOfBooleans( left, right );
    }

    /**
     * Return a {@link PairOfDoubleAndVectorOfDoubles} from a double value and a double vector of values.
     * 
     * @param left the first value
     * @param right the second value
     * @return the pair
     */

    public static PairOfDoubleAndVectorOfDoubles pairOf( double left, double[] right )
    {
        return SafePairOfDoubleAndVectorOfDoubles.of( left, right );
    }

    /**
     * Return a {@link PairOfDoubleAndVectorOfDoubles} from a double value and a double vector of values.
     * 
     * @param left the first value
     * @param right the second value
     * @return the pair
     */

    public static PairOfDoubleAndVectorOfDoubles pairOf( Double left, Double[] right )
    {
        return SafePairOfDoubleAndVectorOfDoubles.of( left, right );
    }

    /**
     * Return a {@link Pair} from two double vectors.
     * 
     * @param left the first value
     * @param right the second value
     * @return the pair
     */

    public static Pair<VectorOfDoubles, VectorOfDoubles> pairOf( double[] left, double[] right )
    {
        return new Pair<VectorOfDoubles, VectorOfDoubles>()
        {

            private static final long serialVersionUID = -1498961647587422087L;

            @Override
            public VectorOfDoubles setValue( VectorOfDoubles vectorOfDoubles )
            {
                throw new UnsupportedOperationException( "Cannot set on this entry." );
            }

            @Override
            public VectorOfDoubles getLeft()
            {
                return VectorOfDoubles.of( left );
            }

            @Override
            public VectorOfDoubles getRight()
            {
                return VectorOfDoubles.of( right );
            }
        };
    }

    /**
     * Return a {@link VectorOfDoubles} from a vector of doubles
     * 
     * @param vec the vector of doubles
     * @return the vector
     */

    public static VectorOfDoubles vectorOf( double[] vec )
    {
        return VectorOfDoubles.of( vec );
    }

    /**
     * Return a {@link VectorOfDoubles} from a vector of doubles
     * 
     * @param vec the vector of doubles
     * @return the vector
     */

    public static VectorOfDoubles vectorOf( Double[] vec )
    {
        return VectorOfDoubles.of( vec );
    }

    /**
     * Return a {@link VectorOfBooleans} from a vector of booleans
     * 
     * @param vec the vector of booleans
     * @return the vector
     */

    public static VectorOfBooleans vectorOf( boolean[] vec )
    {
        return VectorOfBooleans.of( vec );
    }

    /**
     * Return a {@link VectorOfBooleans} from a vector of booleans
     * 
     * @param vec the vector of booleans
     * @return the vector
     */

    public static MatrixOfDoubles matrixOf( double[][] vec )
    {
        return MatrixOfDoubles.of( vec );
    }

    /**
     * Return a {@link DoubleScoreOutput} with a single component. The score component is stored against the 
     * {@link MetricOutputMetadata#getMetricComponentID()} if this is defined, otherwise {@link MetricConstants#MAIN}.
     * 
     * @param output the output data
     * @param meta the metadata
     * @return a {@link DoubleScoreOutput}
     */

    public static DoubleScoreOutput ofDoubleScoreOutput( double output, MetricOutputMetadata meta )
    {
        return new SafeDoubleScoreOutput( output, meta );
    }

    /**
     * Return a {@link DoubleScoreOutput} with multiple components.
     * 
     * @param output the output data
     * @param meta the metadata
     * @return a {@link DoubleScoreOutput}
     */

    public static DoubleScoreOutput ofDoubleScoreOutput( Map<MetricConstants, Double> output,
                                                         MetricOutputMetadata meta )
    {
        return new SafeDoubleScoreOutput( output, meta );
    }

    /**
     * Return a {@link DoubleScoreOutput} with multiple components that are ordered according to the template provided.
     * 
     * @param output the output data
     * @param template the template
     * @param meta the metadata
     * @return a {@link DoubleScoreOutput}
     */

    public static DoubleScoreOutput ofDoubleScoreOutput( double[] output,
                                                         ScoreOutputGroup template,
                                                         MetricOutputMetadata meta )
    {
        return new SafeDoubleScoreOutput( output, template, meta );
    }

    /**
     * Return a {@link MultiVectorOutput}.
     * 
     * @param output the output data
     * @param meta the metadata
     * @return a {@link MultiVectorOutput}
     */

    public static MultiVectorOutput ofMultiVectorOutput( Map<MetricDimension, double[]> output,
                                                         MetricOutputMetadata meta )
    {
        Objects.requireNonNull( output, "Specify a non-null map of inputs." );
        EnumMap<MetricDimension, VectorOfDoubles> map = new EnumMap<>( MetricDimension.class );
        output.forEach( ( key, value ) -> map.put( key, vectorOf( value ) ) );
        return new SafeMultiVectorOutput( map, meta );
    }

    /**
     * Return a {@link MatrixOutput}.
     * 
     * @param output the output data
     * @param names an optional list of names in row-major order, with as many elements as the cardinality of the matrix
     * @param meta the metadata
     * @return a {@link MatrixOutput}
     */

    public static MatrixOutput ofMatrixOutput( double[][] output,
                                               List<MetricDimension> names,
                                               MetricOutputMetadata meta )
    {
        Objects.requireNonNull( output, "Specify a non-null array of inputs." );
        return new SafeMatrixOutput( matrixOf( output ), names, meta );
    }

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

    public static BoxPlotOutput ofBoxPlotOutput( List<PairOfDoubleAndVectorOfDoubles> output,
                                                 VectorOfDoubles probabilities,
                                                 MetricOutputMetadata meta,
                                                 MetricDimension domainAxisDimension,
                                                 MetricDimension rangeAxisDimension )
    {
        return new SafeBoxPlotOutput( output, probabilities, meta, domainAxisDimension, rangeAxisDimension );
    }

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

    public static <S, T> PairedOutput<S, T> ofPairedOutput( List<Pair<S, T>> output,
                                                            MetricOutputMetadata meta )
    {
        return new SafePairedOutput<>( output, meta );
    }

    /**
     * Return a {@link DurationScoreOutput} with a single component. The score component is stored against the 
     * {@link MetricOutputMetadata#getMetricComponentID()} if this is defined, otherwise {@link MetricConstants#MAIN}.
     * 
     * @param output the output data
     * @param meta the metadata
     * @return a {@link DurationScoreOutput}
     */

    public static DurationScoreOutput ofDurationScoreOutput( Duration output, MetricOutputMetadata meta )
    {
        return new SafeDurationScoreOutput( output, meta );
    }

    /**
     * Return a {@link DurationScoreOutput} with multiple components.
     * 
     * @param output the output data
     * @param meta the metadata
     * @return a {@link DurationScoreOutput}
     */

    public static DurationScoreOutput ofDurationScoreOutput( Map<MetricConstants, Duration> output,
                                                             MetricOutputMetadata meta )
    {
        return new SafeDurationScoreOutput( output, meta );
    }

    /**
     * Returns a {@link MapKey} to map a {@link MetricOutput} by an elementary key.
     * 
     * @param <S> the type of key
     * @param key the key
     * @return a map key
     */

    public static <S extends Comparable<S>> MapKey<S> getMapKey( S key )
    {
        return new DefaultMapKey<>( key );
    }

    /**
     * Returns a {@link MetricOutputMapByTimeAndThreshold} from the raw map of inputs.
     * 
     * @param <T> the type of output
     * @param input the map of metric outputs
     * @return a {@link MetricOutputMapByTimeAndThreshold} of metric outputs
     */

    public static <T extends MetricOutput<?>> MetricOutputMapByTimeAndThreshold<T>
            ofMetricOutputMapByTimeAndThreshold( Map<Pair<TimeWindow, OneOrTwoThresholds>, T> input )
    {
        Objects.requireNonNull( input, "Specify a non-null map of inputs by lead time and threshold." );
        final SafeMetricOutputMapByTimeAndThresholdBuilder<T> builder =
                new SafeMetricOutputMapByTimeAndThresholdBuilder<>();
        input.forEach( builder::put );
        return builder.build();
    }

    /**
     * Returns a {@link MetricOutputMultiMapByTimeAndThreshold} from a map of inputs by {@link TimeWindow} and 
     * {@link OneOrTwoThresholds}.
     * 
     * @param <T> the type of output
     * @param input the input map of metric outputs by time window and threshold
     * @return a map of metric outputs by lead time and threshold for several metrics
     * @throws MetricOutputException if attempting to add multiple results for the same metric by time and threshold
     */

    public static <T extends MetricOutput<?>> MetricOutputMultiMapByTimeAndThreshold<T>
            ofMetricOutputMultiMapByTimeAndThreshold( Map<Pair<TimeWindow, OneOrTwoThresholds>, List<MetricOutputMapByMetric<T>>> input )
    {
        Objects.requireNonNull( input, "Specify a non-null map of inputs by threshold." );
        final SafeMetricOutputMultiMapByTimeAndThresholdBuilder<T> builder =
                new SafeMetricOutputMultiMapByTimeAndThresholdBuilder<>();
        input.forEach( ( key, value ) -> {
            //Merge the outputs for different metrics
            final SafeMetricOutputMapByMetricBuilder<T> mBuilder = new SafeMetricOutputMapByMetricBuilder<>();
            value.forEach( mBuilder::put );
            builder.put( key, mBuilder.build() );
        } );
        return builder.build();
    }

    /**
     * Returns a builder for a {@link MetricOutputMultiMapByTimeAndThreshold} that allows for the incremental addition of
     * {@link MetricOutputMapByTimeAndThreshold} as they are computed.
     * 
     * @param <T> the type of output
     * @return a {@link MetricOutputMultiMapByTimeAndThresholdBuilder} for a map of metric outputs by time window and
     *         threshold
     */

    public static <T extends MetricOutput<?>> MetricOutputMultiMapByTimeAndThresholdBuilder<T>
            ofMetricOutputMultiMapByTimeAndThresholdBuilder()
    {
        return new SafeMetricOutputMultiMapByTimeAndThresholdBuilder<>();
    }

    /**
     * Returns a builder for a {@link MetricOutputForProjectByTimeAndThreshold}.
     * 
     * @return a {@link MetricOutputForProjectByTimeAndThresholdBuilder} for a map of metric outputs by time window and
     *         threshold
     */

    public static MetricOutputForProjectByTimeAndThresholdBuilder ofMetricOutputForProjectByTimeAndThreshold()
    {
        return new SafeMetricOutputForProjectByTimeAndThreshold.SafeMetricOutputForProjectByTimeAndThresholdBuilder();
    }

    /**
     * Returns a builder for a {@link TimeSeries} whose timestep may vary.
     * 
     * @param <T> the type of data
     * @return a {@link TimeSeriesBuilder}
     */

    public static <T> TimeSeriesBuilder<T> ofTimeSeriesBuilder()
    {
        return new SafeTimeSeriesBuilder<>();
    }

    /**
     * Returns a builder for a {@link TimeSeriesOfSingleValuedPairs} whose timestep may vary.
     * 
     * @return a {@link TimeSeriesOfSingleValuedPairsBuilder}
     */

    public static TimeSeriesOfSingleValuedPairsBuilder ofTimeSeriesOfSingleValuedPairsBuilder()
    {
        return new SafeTimeSeriesOfSingleValuedPairsBuilder();
    }

    /**
     * Returns a builder for a {@link TimeSeriesOfEnsemblePairs} whose timestep may vary.
     * 
     * @return a {@link TimeSeriesOfEnsemblePairsBuilder}
     */

    public static TimeSeriesOfEnsemblePairsBuilder ofTimeSeriesOfEnsemblePairsBuilder()
    {
        return new SafeTimeSeriesOfEnsemblePairsBuilder();
    }

    /**
     * Returns a {@link MetricOutputMapByMetric} from the raw map of inputs.
     * 
     * @param <T> the type of output
     * @param input the map of metric outputs
     * @return a {@link MetricOutputMapByMetric} of metric outputs
     */

    public static <T extends MetricOutput<?>> MetricOutputMapByMetric<T>
            ofMetricOutputMapByMetric( Map<MetricConstants, T> input )
    {
        Objects.requireNonNull( input, "Specify a non-null list of inputs." );
        final SafeMetricOutputMapByMetricBuilder<T> builder = new SafeMetricOutputMapByMetricBuilder<>();
        input.forEach( ( key, value ) -> builder.put( DataFactory.getMapKey( key ), value ) );
        return builder.build();
    }

    /**
     * Combines a list of {@link MetricOutputMapByTimeAndThreshold} into a single map.
     * 
     * @param <T> the type of output
     * @param input the list of input maps
     * @return a combined {@link MetricOutputMapByTimeAndThreshold} of metric outputs
     */

    public static <T extends MetricOutput<?>> MetricOutputMapByTimeAndThreshold<T>
            combine( List<MetricOutputMapByTimeAndThreshold<T>> input )
    {
        Objects.requireNonNull( input, "Specify a non-null map of inputs to combine." );
        final SafeMetricOutputMapByTimeAndThreshold.SafeMetricOutputMapByTimeAndThresholdBuilder<T> builder =
                new SafeMetricOutputMapByTimeAndThreshold.SafeMetricOutputMapByTimeAndThresholdBuilder<>();
        //If the input contains time windows, find the union of them
        List<TimeWindow> windows = new ArrayList<>();
        for ( MetricOutputMapByTimeAndThreshold<T> next : input )
        {
            next.forEach( builder::put );
            if ( next.getMetadata().hasTimeWindow() )
            {
                windows.add( next.getMetadata().getTimeWindow() );
            }
        }
        MetricOutputMetadata override = input.get( 0 ).getMetadata();
        if ( !windows.isEmpty() )
        {
            override = MetadataFactory.getOutputMetadata( override, TimeWindow.unionOf( windows ) );
        }
        builder.setOverrideMetadata( override );
        return builder.build();
    }

    /**
     * Helper that checks for the equality of two double values using a prescribed number of significant digits.
     * 
     * @param first the first double
     * @param second the second double
     * @param digits the number of significant digits
     * @return true if the first and second are equal to the number of significant digits
     */

    public static boolean doubleEquals( double first, double second, int digits )
    {
        return Math.abs( first - second ) < 1.0 / digits;
    }

    /**
     * Returns an immutable list that contains a safe type of the input.
     * 
     * @param input the possibly unsafe input
     * @return the immutable output
     */

    public static List<PairOfDoubles> safePairOfDoublesList( List<PairOfDoubles> input )
    {
        Objects.requireNonNull( input,
                                "Specify a non-null list of single-valued pairs from which to create a safe type." );
        List<PairOfDoubles> returnMe = new ArrayList<>();
        input.forEach( value -> {
            if ( value instanceof SafePairOfDoubles )
            {
                returnMe.add( value );
            }
            else
            {
                returnMe.add( new SafePairOfDoubles( value.getItemOne(), value.getItemTwo() ) );
            }
        } );
        return Collections.unmodifiableList( returnMe );
    }

    /**
     * Returns an immutable list that contains a safe type of the input.
     * 
     * @param input the possibly unsafe input
     * @return the immutable output
     */

    public static List<PairOfDoubleAndVectorOfDoubles>
            safePairOfDoubleAndVectorOfDoublesList( List<PairOfDoubleAndVectorOfDoubles> input )
    {
        Objects.requireNonNull( input, "Specify a non-null list of ensemble pairs from which to create a safe type." );
        List<PairOfDoubleAndVectorOfDoubles> returnMe = new ArrayList<>();
        input.forEach( value -> {
            if ( value instanceof SafePairOfDoubleAndVectorOfDoubles )
            {
                returnMe.add( value );
            }
            else
            {
                returnMe.add( SafePairOfDoubleAndVectorOfDoubles.of( value.getItemOne(), value.getItemTwo() ) );
            }
        } );
        return Collections.unmodifiableList( returnMe );
    }

    /**
     * Returns an immutable list that contains a safe type of the input.
     * 
     * @param input the possibly unsafe input
     * @return the immutable output
     */

    public static List<VectorOfBooleans> safeVectorOfBooleansList( List<VectorOfBooleans> input )
    {
        Objects.requireNonNull( input,
                                "Specify a non-null list of dichotomous inputs from which to create a safe type." );
        List<VectorOfBooleans> returnMe = new ArrayList<>();
        input.forEach( value -> {
            if ( value instanceof VectorOfBooleans )
            {
                returnMe.add( value );
            }
            else
            {
                returnMe.add( VectorOfBooleans.of( value.getBooleans() ) );
            }
        } );
        return Collections.unmodifiableList( returnMe );
    }

    /**
     * Returns a safe type of the input.
     * 
     * @param input the potentially unsafe input
     * @return a safe implementation of the input
     */

    public static VectorOfDoubles safeVectorOf( VectorOfDoubles input )
    {
        Objects.requireNonNull( input, "Expected non-null input for the safe vector." );
        if ( input instanceof VectorOfDoubles )
        {
            return input;
        }
        return VectorOfDoubles.of( input.getDoubles() );
    }

    /**
     * Consistent comparison of double arrays, first checks count of elements,
     * next goes through values.
     *
     * If first has fewer values, return -1, if first has more values, return 1.
     *
     * If value count is equal, go through in order until an element is less
     * or greater than another. If all values are equal, return 0.
     *
     * @param first the first array
     * @param second the second array
     * @return -1 if first is less than second, 0 if equal, 1 otherwise.
     */
    public static int compareDoubleArray( final double[] first,
                                          final double[] second )
    {
        // this one has fewer elements
        if ( first.length < second.length )
        {
            return -1;
        }
        // this one has more elements
        else if ( first.length > second.length )
        {
            return 1;
        }
        // compare values until we diverge
        else // assumption here is lengths are equal
        {
            for ( int i = 0; i < first.length; i++ )
            {
                int safeComparisonResult = Double.compare( first[i], second[i] );
                if ( safeComparisonResult != 0 )
                {
                    return safeComparisonResult;
                }
            }
            // all values were equal
            return 0;
        }
    }

    /**
     * Default implementation of a pair of booleans.
     */

    private static class SafePairOfBooleans implements PairOfBooleans
    {
        private final boolean left;
        private final boolean right;

        /**
         * Construct.
         * 
         * @param left the left
         * @param right the right
         */

        private SafePairOfBooleans( boolean left, boolean right )
        {
            this.left = left;
            this.right = right;
        }

        @Override
        public boolean getItemOne()
        {
            return left;
        }

        @Override
        public boolean getItemTwo()
        {
            return right;
        }

        @Override
        public boolean equals( Object o )
        {
            if ( ! ( o instanceof SafePairOfBooleans ) )
            {
                return false;
            }
            SafePairOfBooleans b = (SafePairOfBooleans) o;
            return b.getItemOne() == getItemOne() && b.getItemTwo() == getItemTwo();
        }

        @Override
        public int hashCode()
        {
            return Boolean.hashCode( getItemOne() ) + Boolean.hashCode( getItemTwo() );
        }

    };

    /**
     * Default implementation of a {@link MapKey}.
     */

    private static class DefaultMapKey<S extends Comparable<S>> implements MapKey<S>
    {

        /**
         * The map key.
         */

        private final S key;

        DefaultMapKey( S key )
        {
            Objects.requireNonNull( key, "Specify a non-null map key." );
            this.key = key;
        }

        @Override
        public int compareTo( final MapKey<S> o )
        {
            //Compare the keys
            Objects.requireNonNull( o, "Specify a non-null map key for comparison." );
            return getKey().compareTo( o.getKey() );
        }

        @Override
        public boolean equals( Object o )
        {
            if ( ! ( o instanceof DefaultMapKey ) )
            {
                return false;
            }
            DefaultMapKey<?> check = (DefaultMapKey<?>) o;
            return key.equals( check.key );
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode( key );
        }

        @Override
        public S getKey()
        {
            return key;
        }

        @Override
        public String toString()
        {
            return "[" + getKey() + "]";
        }
    }

    /**
     * Prevent construction.
     */

    private DataFactory()
    {
    }

}
