package wres.metrics.discreteprobability;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.math3.util.Precision;

import wres.datamodel.Probability;
import wres.datamodel.MissingValues;
import wres.config.MetricConstants;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolException;
import wres.metrics.Diagram;
import wres.metrics.FunctionFactory;
import wres.metrics.MetricParameterException;
import wres.datamodel.statistics.DiagramStatisticOuter;
import wres.statistics.generated.DiagramMetric;
import wres.statistics.generated.DiagramStatistic;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.DiagramMetric.DiagramMetricComponent;
import wres.statistics.generated.DiagramMetric.DiagramMetricComponent.DiagramComponentType;
import wres.statistics.generated.DiagramStatistic.DiagramStatisticComponent;

/**
 * Computes the Relative Operating Characteristic (ROC; also known as the Receiver Operating Characteristic), which
 * compares the probability of detection (PoFD) against the probability of false detection (PoFD). The empirical ROC is
 * computed for a discrete number of probability thresholds or classifiers that determine whether the forecast event
 * occurred, based on the forecast probability.
 *
 * @author James Brown
 */

public class RelativeOperatingCharacteristicDiagram
        extends Diagram<Pool<Pair<Probability, Probability>>, DiagramStatisticOuter>
{
    /**
     * Probability of detection.
     */

    public static final DiagramMetricComponent PROBABILITY_OF_DETECTION =
            DiagramMetricComponent.newBuilder()
                                  .setName( MetricName.PROBABILITY_OF_DETECTION )
                                  .setType( DiagramComponentType.PRIMARY_RANGE_AXIS )
                                  .setMinimum( MetricConstants.RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM.getMinimum() )
                                  .setMaximum( MetricConstants.RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM.getMaximum() )
                                  .setUnits( "PROBABILITY" )
                                  .build();

    /**
     * Probability of false detection.
     */

    public static final DiagramMetricComponent PROBABILITY_OF_FALSE_DETECTION =
            DiagramMetricComponent.newBuilder()
                                  .setName( MetricName.PROBABILITY_OF_FALSE_DETECTION )
                                  .setType( DiagramComponentType.PRIMARY_DOMAIN_AXIS )
                                  .setMinimum( 0 )
                                  .setMaximum( 1 )
                                  .setUnits( "PROBABILITY" )
                                  .build();

    /**
     * Basic description of the metric.
     */

    public static final DiagramMetric BASIC_METRIC = DiagramMetric.newBuilder()
                                                                  .setName( MetricName.RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM )
                                                                  .setHasDiagonal( true )
                                                                  .build();

    /**
     * Full description of the metric.
     */

    public static final DiagramMetric METRIC = DiagramMetric.newBuilder()
                                                            .addComponents( RelativeOperatingCharacteristicDiagram.PROBABILITY_OF_DETECTION )
                                                            .addComponents( RelativeOperatingCharacteristicDiagram.PROBABILITY_OF_FALSE_DETECTION )
                                                            .setName( MetricName.RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM )
                                                            .setHasDiagonal( true )
                                                            .build();

    /**
     * Default number of points in the diagram.
     */

    private static final int DEFAULT_POINT_COUNT = 10;

    /**
     * Number of points in the empirical ROC diagram.
     */

    private final int points;

    /**
     * Returns an instance.
     *
     * @return an instance
     * @throws MetricParameterException if the metric cannot be constructed
     */

    public static RelativeOperatingCharacteristicDiagram of()
    {
        return new RelativeOperatingCharacteristicDiagram( DEFAULT_POINT_COUNT );
    }

    @Override
    public DiagramStatisticOuter apply( final Pool<Pair<Probability, Probability>> pool )
    {
        if ( Objects.isNull( pool ) )
        {
            throw new PoolException( "Specify non-null input to the '" + this + "'." );
        }

        // Determine the empirical ROC.
        // For each classifier, derive the pairs of booleans and compute the PoD and PoFD from the
        // 2x2 contingency table, using a metric collection to compute the table only once
        double constant = 1.0 / this.points;
        double[] pOD = new double[this.points + 1];
        double[] pOFD = new double[this.points + 1];

        // Initialize arrays
        Arrays.fill( pOD, MissingValues.DOUBLE );
        Arrays.fill( pOFD, MissingValues.DOUBLE );

        // Some data to process        
        if ( !pool.get()
                  .isEmpty() )
        {
            // Calculate the left occurrences, which will be re-used
            List<Pair<Probability, Probability>> poolData = pool.get();
            int poolSize = poolData.size();
            boolean[] left = new boolean[poolSize];
            for ( int i = 0; i < poolSize; i++ )
            {
                Pair<Probability, Probability> p = poolData.get( i );
                left[i] = Double.compare( p.getLeft()
                                           .getProbability(), 1.0 ) == 0;
            }

            // Calculate the 2x2 contingency table components for each predicted threshold
            for ( int i = 1; i < this.points; i++ )
            {
                this.increment( poolData, constant, left, pOD, pOFD, poolSize, i );
            }

            // Set the lower and upper margins to (0.0, 0.0) and (1.0, 1.0), respectively
            pOD[0] = 0.0;
            pOFD[0] = 0.0;
            pOD[this.points] = 1.0;
            pOFD[this.points] = 1.0;
        }

        DiagramStatisticComponent pod =
                DiagramStatisticComponent.newBuilder()
                                         .setMetric( RelativeOperatingCharacteristicDiagram.PROBABILITY_OF_DETECTION )
                                         .addAllValues( Arrays.stream( pOD ).boxed().toList() )
                                         .build();

        DiagramStatisticComponent pofd =
                DiagramStatisticComponent.newBuilder()
                                         .setMetric( RelativeOperatingCharacteristicDiagram.PROBABILITY_OF_FALSE_DETECTION )
                                         .addAllValues( Arrays.stream( pOFD ).boxed().toList() )
                                         .build();

        DiagramStatistic rocDiagram = DiagramStatistic.newBuilder()
                                                      .addStatistics( pod )
                                                      .addStatistics( pofd )
                                                      .setMetric( RelativeOperatingCharacteristicDiagram.BASIC_METRIC )
                                                      .build();

        return DiagramStatisticOuter.of( rocDiagram, pool.getMetadata() );
    }

    @Override
    public MetricConstants getMetricName()
    {
        return MetricConstants.RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM;
    }

    @Override
    public boolean hasRealUnits()
    {
        return false;
    }

    /**
     * Hidden constructor.
     *
     * @param points the number of points in the diagram
     */

    private RelativeOperatingCharacteristicDiagram( int points )
    {
        super();
        //Set the default points
        this.points = points;
    }

    /**
     * Calculate and record the probability of detection and the probability of false detection for the current
     * threshold.
     * @param poolData the pool data
     * @param constant a constant
     * @param left the occurrences for the left values
     * @param pOD the probability of detection to record at the current index
     * @param pOFD the probability of false detection to record at the current index
     * @param poolSize the pool size
     * @param index the index
     */

    private void increment( List<Pair<Probability, Probability>> poolData,
                            double constant,
                            boolean[] left,
                            double[] pOD,
                            double[] pOFD,
                            int poolSize,
                            int index )
    {
        double tP = 0;
        double tN = 0;
        double fP = 0;
        double fN = 0;

        double prob = Precision.round( 1.0 - ( index * constant ), 5 );

        // Compute the PoD/PoFD using the probability threshold to determine whether the event occurred
        // according to the probability on the RHS
        for ( int j = 0; j < poolSize; j++ )
        {
            Pair<Probability, Probability> p = poolData.get( j );
            boolean right = p.getRight()
                             .getProbability() > prob;
            // True positives aka hits
            if ( left[j] && right )
            {
                tP++;
            }
            // True negatives
            else if ( !left[j] && !right )
            {
                tN++;
            }
            // False positives aka false alarms
            else if ( !left[j] )
            {
                fP++;
            }
            // False negatives aka misses
            else
            {
                fN++;
            }
        }

        if ( tP + fN > 0 )
        {
            pOD[index] = FunctionFactory.finiteOrMissing()
                                        .applyAsDouble( tP / ( tP + fN ) );
        }
        if ( fP + tN > 0 )
        {
            pOFD[index] = FunctionFactory.finiteOrMissing()
                                         .applyAsDouble( fP / ( fP + tN ) );
        }
    }
}
