package wres.datamodel;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.MetricConstants.ScoreOutputGroup;
import wres.datamodel.SafeMetricOutputMapByMetric.SafeMetricOutputMapByMetricBuilder;
import wres.datamodel.SafeMetricOutputMultiMapByTimeAndThreshold.SafeMetricOutputMultiMapByTimeAndThresholdBuilder;
import wres.datamodel.SafeRegularTimeSeriesOfEnsemblePairs.SafeRegularTimeSeriesOfEnsemblePairsBuilder;
import wres.datamodel.SafeRegularTimeSeriesOfSingleValuedPairs.SafeRegularTimeSeriesOfSingleValuedPairsBuilder;
import wres.datamodel.SafeTimeSeriesOfEnsemblePairs.SafeTimeSeriesOfEnsemblePairsBuilder;
import wres.datamodel.SafeTimeSeriesOfSingleValuedPairs.SafeTimeSeriesOfSingleValuedPairsBuilder;
import wres.datamodel.Threshold.Operator;
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
import wres.datamodel.inputs.pairs.builders.TimeSeriesOfEnsemblePairsBuilder;
import wres.datamodel.inputs.pairs.builders.TimeSeriesOfSingleValuedPairsBuilder;
import wres.datamodel.metadata.Metadata;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.outputs.BoxPlotOutput;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.datamodel.outputs.DurationScoreOutput;
import wres.datamodel.outputs.MapKey;
import wres.datamodel.outputs.MatrixOutput;
import wres.datamodel.outputs.MetricOutput;
import wres.datamodel.outputs.MetricOutputForProjectByTimeAndThreshold.MetricOutputForProjectByTimeAndThresholdBuilder;
import wres.datamodel.outputs.MetricOutputMapByMetric;
import wres.datamodel.outputs.MetricOutputMapByTimeAndThreshold;
import wres.datamodel.outputs.MetricOutputMultiMapByTimeAndThreshold;
import wres.datamodel.outputs.MultiVectorOutput;
import wres.datamodel.outputs.PairedOutput;
import wres.datamodel.time.Event;

/**
 * A default factory class for producing metric inputs.
 * 
 * @author james.brown@hydrosolved.com
 * @author jesse
 * @version 0.1
 * @since 0.1
 */

public class DefaultDataFactory implements DataFactory
{

    /**
     * Instance of the factory.
     */

    private static DataFactory instance = null;

    /**
     * Returns an instance of a {@link DataFactory}.
     * 
     * @return a {@link DataFactory}
     */

    public static DataFactory getInstance()
    {
        if ( Objects.isNull( instance ) )
        {
            instance = new DefaultDataFactory();
        }
        return instance;
    }

    @Override
    public MetadataFactory getMetadataFactory()
    {
        return DefaultMetadataFactory.getInstance();
    }

    @Override
    public Slicer getSlicer()
    {
        return DefaultSlicer.getInstance();
    }

    @Override
    public DichotomousPairs ofDichotomousPairs( List<VectorOfBooleans> pairs,
                                                List<VectorOfBooleans> basePairs,
                                                Metadata mainMeta,
                                                Metadata baselineMeta,
                                                VectorOfDoubles climatology )
    {
        final SafeDichotomousPairs.DichotomousPairsBuilder b = new SafeDichotomousPairs.DichotomousPairsBuilder();
        return (DichotomousPairs) b.addData( pairs )
                                   .setMetadata( mainMeta )
                                   .addDataForBaseline( basePairs )
                                   .setMetadataForBaseline( baselineMeta )
                                   .setClimatology( climatology )
                                   .build();
    }

    @Override
    public DichotomousPairs ofDichotomousPairsFromAtomic( List<PairOfBooleans> pairs,
                                                          List<PairOfBooleans> basePairs,
                                                          Metadata mainMeta,
                                                          Metadata baselineMeta,
                                                          VectorOfDoubles climatology )
    {
        final SafeDichotomousPairs.DichotomousPairsBuilder b = new SafeDichotomousPairs.DichotomousPairsBuilder();
        b.setDataFromAtomic( pairs ).setMetadata( mainMeta ).setClimatology( climatology );
        return (DichotomousPairs) b.setDataForBaselineFromAtomic( basePairs )
                                   .setMetadataForBaseline( baselineMeta )
                                   .build();
    }

    @Override
    public MulticategoryPairs ofMulticategoryPairs( List<VectorOfBooleans> pairs,
                                                    List<VectorOfBooleans> basePairs,
                                                    Metadata mainMeta,
                                                    Metadata baselineMeta,
                                                    VectorOfDoubles climatology )
    {
        final SafeMulticategoryPairs.MulticategoryPairsBuilder b =
                new SafeMulticategoryPairs.MulticategoryPairsBuilder();
        return (MulticategoryPairs) b.addData( pairs )
                                     .setMetadata( mainMeta )
                                     .addDataForBaseline( basePairs )
                                     .setMetadataForBaseline( baselineMeta )
                                     .setClimatology( climatology )
                                     .build();
    }

    @Override
    public DiscreteProbabilityPairs ofDiscreteProbabilityPairs( final List<PairOfDoubles> pairs,
                                                                final List<PairOfDoubles> basePairs,
                                                                final Metadata mainMeta,
                                                                final Metadata baselineMeta,
                                                                VectorOfDoubles climatology )
    {
        final SafeDiscreteProbabilityPairs.DiscreteProbabilityPairsBuilder b =
                new SafeDiscreteProbabilityPairs.DiscreteProbabilityPairsBuilder();
        return (DiscreteProbabilityPairs) b.addData( pairs )
                                           .setMetadata( mainMeta )
                                           .addDataForBaseline( basePairs )
                                           .setMetadataForBaseline( baselineMeta )
                                           .setClimatology( climatology )
                                           .build();
    }

    @Override
    public SingleValuedPairs ofSingleValuedPairs( final List<PairOfDoubles> pairs,
                                                  final List<PairOfDoubles> basePairs,
                                                  final Metadata mainMeta,
                                                  final Metadata baselineMeta,
                                                  VectorOfDoubles climatology )
    {
        final SafeSingleValuedPairs.SingleValuedPairsBuilder b = new SafeSingleValuedPairs.SingleValuedPairsBuilder();
        return (SingleValuedPairs) b.setMetadata( mainMeta )
                                    .addData( pairs )
                                    .addDataForBaseline( basePairs )
                                    .setMetadataForBaseline( baselineMeta )
                                    .setClimatology( climatology )
                                    .build();
    }

    @Override
    public EnsemblePairs ofEnsemblePairs( final List<PairOfDoubleAndVectorOfDoubles> pairs,
                                          final List<PairOfDoubleAndVectorOfDoubles> basePairs,
                                          final Metadata mainMeta,
                                          final Metadata baselineMeta,
                                          VectorOfDoubles climatology )
    {
        final SafeEnsemblePairs.EnsemblePairsBuilder b = new SafeEnsemblePairs.EnsemblePairsBuilder();
        return (EnsemblePairs) b.setMetadata( mainMeta )
                                .addData( pairs )
                                .addDataForBaseline( basePairs )
                                .setMetadataForBaseline( baselineMeta )
                                .setClimatology( climatology )
                                .build();
    }

    @Override
    public PairOfDoubles pairOf( final double left, final double right )
    {
        return new SafePairOfDoubles( left, right );
    }

    @Override
    public PairOfBooleans pairOf( final boolean left, final boolean right )
    {
        return new SafePairOfBooleans( left, right );
    }

    @Override
    public PairOfDoubleAndVectorOfDoubles pairOf( final double left, final double[] right )
    {
        return SafePairOfDoubleAndVectorOfDoubles.of( left, right );
    }

    @Override
    public PairOfDoubleAndVectorOfDoubles pairOf( final Double left, final Double[] right )
    {
        return SafePairOfDoubleAndVectorOfDoubles.of( left, right );
    }

    @Override
    public Pair<VectorOfDoubles, VectorOfDoubles> pairOf( final double[] left, final double[] right )
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
                return SafeVectorOfDoubles.of( left );
            }

            @Override
            public VectorOfDoubles getRight()
            {
                return SafeVectorOfDoubles.of( right );
            }
        };
    }

    @Override
    public VectorOfDoubles vectorOf( final double[] vec )
    {
        return SafeVectorOfDoubles.of( vec );
    }

    @Override
    public VectorOfDoubles vectorOf( final Double[] vec )
    {
        return SafeVectorOfDoubles.of( vec );
    }

    @Override
    public VectorOfBooleans vectorOf( final boolean[] vec )
    {
        return SafeVectorOfBooleans.of( vec );
    }

    @Override
    public MatrixOfDoubles matrixOf( final double[][] vec )
    {
        return SafeMatrixOfDoubles.of( vec );
    }

    @Override
    public DoubleScoreOutput ofDoubleScoreOutput( final double output, final MetricOutputMetadata meta )
    {
        return new SafeDoubleScoreOutput( output, meta );
    }

    @Override
    public DoubleScoreOutput ofDoubleScoreOutput( Map<MetricConstants, Double> output, MetricOutputMetadata meta )
    {
        return new SafeDoubleScoreOutput( output, meta );
    }

    @Override
    public DurationScoreOutput ofDurationScoreOutput( Map<MetricConstants, Duration> output, MetricOutputMetadata meta )
    {
        return new SafeDurationScoreOutput( output, meta );
    }

    @Override
    public DoubleScoreOutput ofDoubleScoreOutput( double[] output,
                                                  ScoreOutputGroup template,
                                                  MetricOutputMetadata meta )
    {
        return new SafeDoubleScoreOutput( output, template, meta );
    }

    @Override
    public MultiVectorOutput ofMultiVectorOutput( final Map<MetricDimension, double[]> output,
                                                  final MetricOutputMetadata meta )
    {
        Objects.requireNonNull( output, "Specify a non-null map of inputs." );
        EnumMap<MetricDimension, VectorOfDoubles> map = new EnumMap<>( MetricDimension.class );
        output.forEach( ( key, value ) -> map.put( key, vectorOf( value ) ) );
        return new SafeMultiVectorOutput( map, meta );
    }

    @Override
    public MatrixOutput ofMatrixOutput( final double[][] output,
                                        final List<MetricDimension> names,
                                        final MetricOutputMetadata meta )
    {
        Objects.requireNonNull( output, "Specify a non-null array of inputs." );
        return new SafeMatrixOutput( matrixOf( output ), names, meta );
    }

    @Override
    public BoxPlotOutput ofBoxPlotOutput( List<PairOfDoubleAndVectorOfDoubles> output,
                                          VectorOfDoubles probabilities,
                                          MetricOutputMetadata meta,
                                          MetricDimension domainAxisDimension,
                                          MetricDimension rangeAxisDimension )
    {
        return new SafeBoxPlotOutput( output, probabilities, meta, domainAxisDimension, rangeAxisDimension );
    }

    @Override
    public <S, T> PairedOutput<S, T> ofPairedOutput( List<Pair<S, T>> output, MetricOutputMetadata meta )
    {
        return new SafePairedOutput<>( output, meta );
    }

    @Override
    public DurationScoreOutput ofDurationScoreOutput( Duration output, MetricOutputMetadata meta )
    {
        return new SafeDurationScoreOutput( output, meta );
    }

    @Override
    public <T extends MetricOutput<?>> MetricOutputMapByMetric<T> ofMap( final List<T> input )
    {
        Objects.requireNonNull( input, "Specify a non-null list of inputs." );
        final SafeMetricOutputMapByMetricBuilder<T> builder = new SafeMetricOutputMapByMetricBuilder<>();
        input.forEach( a -> {
            final MapKey<MetricConstants> key = getMapKey( a.getMetadata().getMetricID() );
            builder.put( key, a );
        } );
        return builder.build();
    }

    @Override
    public <T extends MetricOutput<?>> MetricOutputMapByTimeAndThreshold<T>
            ofMap( final Map<Pair<TimeWindow, Thresholds>, T> input )
    {
        Objects.requireNonNull( input, "Specify a non-null map of inputs by lead time and threshold." );
        final SafeMetricOutputMapByTimeAndThreshold.Builder<T> builder =
                new SafeMetricOutputMapByTimeAndThreshold.Builder<>();
        input.forEach( builder::put );
        return builder.build();
    }

    @Override
    public <T extends MetricOutput<?>> MetricOutputMultiMapByTimeAndThreshold<T>
            ofMultiMap( final Map<Pair<TimeWindow, Thresholds>, List<MetricOutputMapByMetric<T>>> input )
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

    @Override
    public <S extends MetricOutput<?>>
            MetricOutputMultiMapByTimeAndThreshold.MetricOutputMultiMapByTimeAndThresholdBuilder<S>
            ofMultiMap()
    {
        return new SafeMetricOutputMultiMapByTimeAndThresholdBuilder<>();
    }


    @Override
    public TimeSeriesOfSingleValuedPairs
            ofRegularTimeSeriesOfSingleValuedPairs( List<Event<List<PairOfDoubles>>> timeSeries,
                                                    Metadata mainMeta,
                                                    List<Event<List<PairOfDoubles>>> timeSeriesBaseline,
                                                    Metadata baselineMeta,
                                                    Duration timeStep )
    {
        SafeRegularTimeSeriesOfSingleValuedPairsBuilder builder = new SafeRegularTimeSeriesOfSingleValuedPairsBuilder();
        builder.addTimeSeriesData( timeSeries ).setTimeStep( timeStep ).setMetadata( mainMeta );
        if ( Objects.nonNull( timeSeriesBaseline ) )
        {
            builder.addTimeSeriesDataForBaseline( timeSeriesBaseline );
            builder.setMetadataForBaseline( baselineMeta );
        }
        return builder.build();
    }

    @Override
    public TimeSeriesOfEnsemblePairs
            ofRegularTimeSeriesOfEnsemblePairs( List<Event<List<PairOfDoubleAndVectorOfDoubles>>> timeSeries,
                                                Metadata mainMeta,
                                                List<Event<List<PairOfDoubleAndVectorOfDoubles>>> timeSeriesBaseline,
                                                Metadata baselineMeta,
                                                Duration timeStep )
    {
        SafeRegularTimeSeriesOfEnsemblePairsBuilder builder = new SafeRegularTimeSeriesOfEnsemblePairsBuilder();
        builder.addTimeSeriesData( timeSeries ).setTimeStep( timeStep ).setMetadata( mainMeta );
        if ( Objects.nonNull( timeSeriesBaseline ) )
        {
            builder.addTimeSeriesDataForBaseline( timeSeriesBaseline );
            builder.setMetadataForBaseline( baselineMeta );
        }
        return builder.build();
    }


    @Override
    public RegularTimeSeriesOfSingleValuedPairsBuilder ofRegularTimeSeriesOfSingleValuedPairsBuilder()
    {
        return new SafeRegularTimeSeriesOfSingleValuedPairsBuilder();
    }

    @Override
    public RegularTimeSeriesOfEnsemblePairsBuilder ofRegularTimeSeriesOfEnsemblePairsBuilder()
    {
        return new SafeRegularTimeSeriesOfEnsemblePairsBuilder();
    }


    @Override
    public TimeSeriesOfSingleValuedPairs ofTimeSeriesOfSingleValuedPairs(
                                                                          List<Event<List<Event<PairOfDoubles>>>> timeSeries,
                                                                          Metadata mainMeta,
                                                                          List<Event<List<Event<PairOfDoubles>>>> timeSeriesBaseline,
                                                                          Metadata baselineMeta )
    {
        SafeTimeSeriesOfSingleValuedPairsBuilder builder = new SafeTimeSeriesOfSingleValuedPairsBuilder();
        builder.addTimeSeriesData( timeSeries ).setMetadata( mainMeta );
        if ( Objects.nonNull( timeSeriesBaseline ) )
        {
            builder.addTimeSeriesDataForBaseline( timeSeriesBaseline );
            builder.setMetadataForBaseline( baselineMeta );
        }
        return builder.build();
    }

    @Override
    public TimeSeriesOfEnsemblePairs ofTimeSeriesOfEnsemblePairs(
                                                                  List<Event<List<Event<PairOfDoubleAndVectorOfDoubles>>>> timeSeries,
                                                                  Metadata mainMeta,
                                                                  List<Event<List<Event<PairOfDoubleAndVectorOfDoubles>>>> timeSeriesBaseline,
                                                                  Metadata baselineMeta )
    {
        SafeTimeSeriesOfEnsemblePairsBuilder builder = new SafeTimeSeriesOfEnsemblePairsBuilder();
        builder.addTimeSeriesData( timeSeries ).setMetadata( mainMeta );
        if ( Objects.nonNull( timeSeriesBaseline ) )
        {
            builder.addTimeSeriesDataForBaseline( timeSeriesBaseline );
            builder.setMetadataForBaseline( baselineMeta );
        }
        return builder.build();
    }

    @Override
    public TimeSeriesOfSingleValuedPairsBuilder ofTimeSeriesOfSingleValuedPairsBuilder()
    {
        return new SafeTimeSeriesOfSingleValuedPairsBuilder();
    }

    @Override
    public TimeSeriesOfEnsemblePairsBuilder ofTimeSeriesOfEnsemblePairsBuilder()
    {
        return new SafeTimeSeriesOfEnsemblePairsBuilder();
    }

    @Override
    public <T extends MetricOutput<?>> MetricOutputMapByTimeAndThreshold<T>
            combine( final List<MetricOutputMapByTimeAndThreshold<T>> input )
    {
        Objects.requireNonNull( input, "Specify a non-null map of inputs to combine." );
        final SafeMetricOutputMapByTimeAndThreshold.Builder<T> builder =
                new SafeMetricOutputMapByTimeAndThreshold.Builder<>();
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
            override = getMetadataFactory().getOutputMetadata( override, TimeWindow.unionOf( windows ) );
        }
        builder.setOverrideMetadata( override );
        return builder.build();
    }

    @Override
    public <S extends Comparable<S>> MapKey<S> getMapKey( final S key )
    {
        return new DefaultMapKey<>( key );
    }

    @Override
    public Threshold ofThreshold( final Double threshold, final Double thresholdUpper, final Operator condition, String label )
    {
        return new SafeThreshold.ThresholdBuilder().setThreshold( threshold )
                                                   .setThresholdUpper( thresholdUpper )
                                                   .setCondition( condition )
                                                   .setLabel( label)
                                                   .build();
    }

    @Override
    public Threshold ofProbabilityThreshold( final Double threshold,
                                              final Double thresholdUpper,
                                              final Operator condition,
                                              final String label )
    {
        return new SafeThreshold.ThresholdBuilder().setThresholdProbability( threshold )
                                                   .setThresholdProbabilityUpper( thresholdUpper )
                                                   .setCondition( condition )
                                                   .setLabel( label)
                                                   .build();
    }

    @Override
    public Threshold ofQuantileThreshold( final Double threshold,
                                           final Double thresholdUpper,
                                           final Double probability,
                                           final Double probabilityUpper,
                                           final Operator condition,
                                           final String label )
    {
        return new SafeThreshold.ThresholdBuilder().setThreshold( threshold )
                                                   .setThresholdUpper( thresholdUpper )
                                                   .setThresholdProbability( probability )
                                                   .setThresholdProbabilityUpper( probabilityUpper )
                                                   .setCondition( condition )
                                                   .setLabel( label)
                                                   .build();
    }

    @Override
    public MetricOutputForProjectByTimeAndThresholdBuilder ofMetricOutputForProjectByTimeAndThreshold()
    {
        return new SafeMetricOutputForProjectByTimeAndThreshold.SafeMetricOutputForProjectByTimeAndThresholdBuilder();
    }

    @Override
    public boolean doubleEquals( double first, double second, int digits )
    {
        return Math.abs( first - second ) < 1.0 / digits;
    }

    /**
     * Returns an immutable list that contains a safe type of the input.
     * 
     * @param input the possibly unsafe input
     * @return the immutable output
     */

    List<PairOfDoubles> safePairOfDoublesList( List<PairOfDoubles> input )
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

    List<PairOfDoubleAndVectorOfDoubles>
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

    List<VectorOfBooleans> safeVectorOfBooleansList( List<VectorOfBooleans> input )
    {
        Objects.requireNonNull( input,
                                "Specify a non-null list of dichotomous inputs from which to create a safe type." );
        List<VectorOfBooleans> returnMe = new ArrayList<>();
        input.forEach( value -> {
            if ( value instanceof SafeVectorOfBooleans )
            {
                returnMe.add( value );
            }
            else
            {
                returnMe.add( SafeVectorOfBooleans.of( value.getBooleans() ) );
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

    VectorOfDoubles safeVectorOf( VectorOfDoubles input )
    {
        Objects.requireNonNull( input, "Expected non-null input for the safe vector." );
        if ( input instanceof SafeVectorOfDoubles )
        {
            return input;
        }
        return SafeVectorOfDoubles.of( input.getDoubles() );
    }

    /**
     * Returns a safe type of the input.
     * 
     * @param input the potentially unsafe input
     * @return a safe implementation of the input
     */

    MatrixOfDoubles safeMatrixOf( MatrixOfDoubles input )
    {
        Objects.requireNonNull( input, "Expected non-null input for the safe matrix." );
        if ( input instanceof SafeMatrixOfDoubles )
        {
            return input;
        }
        return SafeMatrixOfDoubles.of( input.getDoubles() );
    }

    /**
     * Default implementation of a pair of booleans.
     */

    class SafePairOfBooleans implements PairOfBooleans
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

    class DefaultMapKey<S extends Comparable<S>> implements MapKey<S>
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

    private DefaultDataFactory()
    {
    }

}
