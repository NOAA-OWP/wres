package wres.datamodel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.MetricConstants.MetricOutputGroup;
import wres.datamodel.SafeMetricOutputMultiMapByTimeAndThreshold.SafeMetricOutputMultiMapByTimeAndThresholdBuilder;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.outputs.BoxPlotOutput;
import wres.datamodel.outputs.MatrixOutput;
import wres.datamodel.outputs.MetricOutput;
import wres.datamodel.outputs.MetricOutputAccessException;
import wres.datamodel.outputs.MetricOutputForProjectByTimeAndThreshold;
import wres.datamodel.outputs.MetricOutputMapByMetric;
import wres.datamodel.outputs.MetricOutputMultiMapByTimeAndThreshold;
import wres.datamodel.outputs.MetricOutputMultiMapByTimeAndThreshold.MetricOutputMultiMapByTimeAndThresholdBuilder;
import wres.datamodel.outputs.MultiVectorOutput;
import wres.datamodel.outputs.ScalarOutput;
import wres.datamodel.outputs.VectorOutput;

/**
 * <p>
 * An immutable implementation of a {@link MetricOutputForProjectByTimeAndThreshold}.
 * </p>
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

class SafeMetricOutputForProjectByTimeAndThreshold implements MetricOutputForProjectByTimeAndThreshold
{

    /**
     * Thread safe map for {@link ScalarOutput}.
     */

    private final ConcurrentMap<Pair<TimeWindow, Threshold>, List<Future<MetricOutputMapByMetric<ScalarOutput>>>> scalar =
            new ConcurrentHashMap<>();

    /**
     * Thread safe map for {@link VectorOutput}.
     */

    private final ConcurrentMap<Pair<TimeWindow, Threshold>, List<Future<MetricOutputMapByMetric<VectorOutput>>>> vector =
            new ConcurrentHashMap<>();

    /**
     * Thread safe map for {@link MultiVectorOutput}.
     */

    private final ConcurrentMap<Pair<TimeWindow, Threshold>, List<Future<MetricOutputMapByMetric<MultiVectorOutput>>>> multiVector =
            new ConcurrentHashMap<>();

    /**
     * Thread safe map for {@link MatrixOutput}.
     */

    private final ConcurrentMap<Pair<TimeWindow, Threshold>, List<Future<MetricOutputMapByMetric<MatrixOutput>>>> matrix =
            new ConcurrentHashMap<>();

    /**
     * Thread safe map for {@link BoxPlotOutput}.
     */

    private final ConcurrentMap<Pair<TimeWindow, Threshold>, List<Future<MetricOutputMapByMetric<BoxPlotOutput>>>> boxplot =
            new ConcurrentHashMap<>();


    @Override
    public boolean hasOutput( MetricOutputGroup outGroup )
    {
        switch ( outGroup )
        {
            case SCALAR:
                return !scalar.isEmpty();
            case VECTOR:
                return !vector.isEmpty();
            case MULTIVECTOR:
                return !multiVector.isEmpty();
            case MATRIX:
                return !matrix.isEmpty();
            case BOXPLOT:
                return !boxplot.isEmpty();
            default:
                return false;
        }
    }

    @Override
    public MetricOutputMultiMapByTimeAndThreshold<MetricOutput<?>> getOutput( MetricOutputGroup... outGroup )
            throws MetricOutputAccessException
    {
        Objects.requireNonNull( outGroup, "Specify one or more output types to return." );
        SafeMetricOutputMultiMapByTimeAndThresholdBuilder<MetricOutput<?>> builder =
                new SafeMetricOutputMultiMapByTimeAndThresholdBuilder<>();
        //Iterate through the types
        for ( MetricOutputGroup next : outGroup )
        {
            if ( hasOutput( next ) )
            {
                switch ( next )
                {
                    case SCALAR:
                        addToBuilder( builder, getScalarOutput() );
                        break;
                    case VECTOR:
                        addToBuilder( builder, getVectorOutput() );
                        break;
                    case MULTIVECTOR:
                        addToBuilder( builder, getMultiVectorOutput() );
                        break;
                    case MATRIX:
                        addToBuilder( builder, getMatrixOutput() );
                        break;
                    case BOXPLOT:
                        addToBuilder( builder, getBoxPlotOutput() );
                        break;
                }
            }
        }
        return builder.build();
    }

    @Override
    public MetricOutputGroup[] getOutputTypes()
    {
        List<MetricOutputGroup> returnMe = new ArrayList<>();
        if ( hasOutput( MetricOutputGroup.SCALAR ) )
        {
            returnMe.add( MetricOutputGroup.SCALAR );
        }
        if ( hasOutput( MetricOutputGroup.VECTOR ) )
        {
            returnMe.add( MetricOutputGroup.VECTOR );
        }
        if ( hasOutput( MetricOutputGroup.MULTIVECTOR ) )
        {
            returnMe.add( MetricOutputGroup.MULTIVECTOR );
        }
        if ( hasOutput( MetricOutputGroup.MATRIX ) )
        {
            returnMe.add( MetricOutputGroup.MATRIX );
        }
        if ( hasOutput( MetricOutputGroup.BOXPLOT ) )
        {
            returnMe.add( MetricOutputGroup.BOXPLOT );
        }
        return returnMe.toArray( new MetricOutputGroup[returnMe.size()] );
    }

    @Override
    public MetricOutputMultiMapByTimeAndThreshold<ScalarOutput> getScalarOutput() throws MetricOutputAccessException
    {
        return unwrap( MetricOutputGroup.SCALAR, scalar );
    }

    @Override
    public MetricOutputMultiMapByTimeAndThreshold<VectorOutput> getVectorOutput() throws MetricOutputAccessException
    {
        return unwrap( MetricOutputGroup.VECTOR, vector );
    }

    @Override
    public MetricOutputMultiMapByTimeAndThreshold<MultiVectorOutput> getMultiVectorOutput()
            throws MetricOutputAccessException
    {
        return unwrap( MetricOutputGroup.MULTIVECTOR, multiVector );
    }

    @Override
    public MetricOutputMultiMapByTimeAndThreshold<MatrixOutput> getMatrixOutput() throws MetricOutputAccessException
    {
        return unwrap( MetricOutputGroup.MATRIX, matrix );
    }

    @Override
    public MetricOutputMultiMapByTimeAndThreshold<BoxPlotOutput> getBoxPlotOutput() throws MetricOutputAccessException
    {
        return unwrap( MetricOutputGroup.BOXPLOT, boxplot );
    }

    /**
     * Builder.
     */

    static class SafeMetricOutputForProjectByTimeAndThresholdBuilder
            implements MetricOutputForProjectByTimeAndThresholdBuilder
    {

        /**
         * Thread safe map for {@link ScalarOutput}.
         */

        private final ConcurrentMap<Pair<TimeWindow, Threshold>, List<Future<MetricOutputMapByMetric<ScalarOutput>>>> scalarInternal =
                new ConcurrentHashMap<>();

        /**
         * Thread safe map for {@link VectorOutput}.
         */

        private final ConcurrentMap<Pair<TimeWindow, Threshold>, List<Future<MetricOutputMapByMetric<VectorOutput>>>> vectorInternal =
                new ConcurrentHashMap<>();

        /**
         * Thread safe map for {@link MultiVectorOutput}.
         */

        private final ConcurrentMap<Pair<TimeWindow, Threshold>, List<Future<MetricOutputMapByMetric<MultiVectorOutput>>>> multiVectorInternal =
                new ConcurrentHashMap<>();

        /**
         * Thread safe map for {@link MatrixOutput}.
         */

        private final ConcurrentMap<Pair<TimeWindow, Threshold>, List<Future<MetricOutputMapByMetric<MatrixOutput>>>> matrixInternal =
                new ConcurrentHashMap<>();

        /**
         * Thread safe map for {@link BoxPlotOutput}.
         */

        private final ConcurrentMap<Pair<TimeWindow, Threshold>, List<Future<MetricOutputMapByMetric<BoxPlotOutput>>>> boxplotInternal =
                new ConcurrentHashMap<>();

        @Override
        public MetricOutputForProjectByTimeAndThresholdBuilder addScalarOutput( TimeWindow timeWindow,
                                                                                Threshold threshold,
                                                                                Future<MetricOutputMapByMetric<ScalarOutput>> result )
        {
            List<Future<MetricOutputMapByMetric<ScalarOutput>>> existing =
                    scalarInternal.putIfAbsent( Pair.of( timeWindow, threshold ),
                                                new ArrayList<>( Arrays.asList( result ) ) );
            if ( Objects.nonNull( existing ) )
            {
                existing.add( result );
            }
            return this;
        }

        @Override
        public MetricOutputForProjectByTimeAndThresholdBuilder addVectorOutput( TimeWindow timeWindow,
                                                                                Threshold threshold,
                                                                                Future<MetricOutputMapByMetric<VectorOutput>> result )
        {
            List<Future<MetricOutputMapByMetric<VectorOutput>>> existing =
                    vectorInternal.putIfAbsent( Pair.of( timeWindow, threshold ),
                                                new ArrayList<>( Arrays.asList( result ) ) );
            if ( Objects.nonNull( existing ) )
            {
                existing.add( result );
            }
            return this;
        }

        @Override
        public MetricOutputForProjectByTimeAndThresholdBuilder addMultiVectorOutput( TimeWindow timeWindow,
                                                                                     Threshold threshold,
                                                                                     Future<MetricOutputMapByMetric<MultiVectorOutput>> result )
        {
            List<Future<MetricOutputMapByMetric<MultiVectorOutput>>> existing =
                    multiVectorInternal.putIfAbsent( Pair.of( timeWindow, threshold ),
                                                     new ArrayList<>( Arrays.asList( result ) ) );
            if ( Objects.nonNull( existing ) )
            {
                existing.add( result );
            }
            return this;
        }

        @Override
        public MetricOutputForProjectByTimeAndThresholdBuilder addMatrixOutput( TimeWindow timeWindow,
                                                                                Threshold threshold,
                                                                                Future<MetricOutputMapByMetric<MatrixOutput>> result )
        {
            List<Future<MetricOutputMapByMetric<MatrixOutput>>> existing =
                    matrixInternal.putIfAbsent( Pair.of( timeWindow, threshold ),
                                                new ArrayList<>( Arrays.asList( result ) ) );
            if ( Objects.nonNull( existing ) )
            {
                existing.add( result );
            }
            return this;
        }

        @Override
        public MetricOutputForProjectByTimeAndThresholdBuilder addBoxPlotOutput( TimeWindow timeWindow,
                                                                                 Threshold threshold,
                                                                                 Future<MetricOutputMapByMetric<BoxPlotOutput>> result )
        {
            List<Future<MetricOutputMapByMetric<BoxPlotOutput>>> existing =
                    boxplotInternal.putIfAbsent( Pair.of( timeWindow, threshold ),
                                                 new ArrayList<>( Arrays.asList( result ) ) );
            if ( Objects.nonNull( existing ) )
            {
                existing.add( result );
            }
            return this;
        }

        @Override
        public MetricOutputForProjectByTimeAndThreshold build()
        {
            return new SafeMetricOutputForProjectByTimeAndThreshold( this );
        }

    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     */

    private SafeMetricOutputForProjectByTimeAndThreshold( SafeMetricOutputForProjectByTimeAndThresholdBuilder builder )
    {
        scalar.putAll( builder.scalarInternal );
        vector.putAll( builder.vectorInternal );
        multiVector.putAll( builder.multiVectorInternal );
        matrix.putAll( builder.matrixInternal );
        boxplot.putAll( builder.boxplotInternal );
    }

    /**
     * Helper that adds an existing output collection to an existing map.
     *
     * @param builder the builder
     * @param addMe the metric output collection
     */

    private void addToBuilder( MetricOutputMultiMapByTimeAndThresholdBuilder<MetricOutput<?>> builder,
                               MetricOutputMultiMapByTimeAndThreshold<?> addMe )
    {
        DataFactory d = DefaultDataFactory.getInstance();
        addMe.forEach( ( key, value ) -> {
            Map<Pair<TimeWindow, Threshold>, MetricOutput<?>> map = new TreeMap<>();
            value.forEach( map::put );
            builder.put( key, d.ofMap( map ) );
        } );
    }

    /**
     * Unwraps a map of values that are wrapped in {@link Future} by calling {@link Future#get()} on each value and
     * returning a map of the unwrapped entries.
     * 
     * @param <T> the type of output
     * @param outGroup the {@link MetricOutputGroup} for error logging
     * @param wrapped the map of values wrapped in {@link Future}
     * @return the unwrapped map or null if the input is empty
     * @throws MetricOutputAccessException if the retrieval of {@link MetricOutput} fails
     */

    private <T extends MetricOutput<?>> MetricOutputMultiMapByTimeAndThreshold<T> unwrap( MetricOutputGroup outGroup,
                                                                                          Map<Pair<TimeWindow, Threshold>, List<Future<MetricOutputMapByMetric<T>>>> wrapped )
            throws MetricOutputAccessException
    {
        if ( wrapped.isEmpty() )
        {
            return null;
        }
        DataFactory d = DefaultDataFactory.getInstance();
        Map<Pair<TimeWindow, Threshold>, List<MetricOutputMapByMetric<T>>> unwrapped = new HashMap<>();
        for ( Map.Entry<Pair<TimeWindow, Threshold>, List<Future<MetricOutputMapByMetric<T>>>> next : wrapped.entrySet() )
        {
            try
            {
                for ( Future<MetricOutputMapByMetric<T>> nextEntry : next.getValue() )
                {
                    List<MetricOutputMapByMetric<T>> existing =
                            unwrapped.putIfAbsent( next.getKey(), new ArrayList<>( Arrays.asList( nextEntry.get() ) ) );
                    if ( Objects.nonNull( existing ) )
                    {
                        existing.add( nextEntry.get() );
                    }
                }
            }
            catch ( InterruptedException e )
            {
                Thread.currentThread().interrupt();
                throw new MetricOutputAccessException( "Interrupted while retrieving the results for group " + outGroup
                                                       + " "
                                                       + "at lead time "
                                                       + next.getKey().getLeft()
                                                       + " and threshold "
                                                       + next.getKey().getRight()
                                                       + ".",
                                                       e );
            }
            catch ( ExecutionException e )
            {
                throw new MetricOutputAccessException( "While retrieving the results for group " + outGroup
                                                       + " at lead time "
                                                       + next.getKey().getLeft()
                                                       + " and threshold "
                                                       + next.getKey().getRight()
                                                       + ".",
                                                       e );
            }
        }
        return d.ofMultiMap( unwrapped );
    }

}
