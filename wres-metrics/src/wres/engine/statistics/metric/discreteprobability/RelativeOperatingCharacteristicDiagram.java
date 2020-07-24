package wres.engine.statistics.metric.discreteprobability;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.math3.util.Precision;

import wres.datamodel.MetricConstants;
import wres.datamodel.Probability;
import wres.datamodel.MissingValues;
import wres.datamodel.Slicer;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.datamodel.statistics.DiagramStatisticOuter;
import wres.engine.statistics.metric.Diagram;
import wres.engine.statistics.metric.MetricCollection;
import wres.engine.statistics.metric.MetricFactory;
import wres.engine.statistics.metric.MetricParameterException;
import wres.statistics.generated.DiagramMetric;
import wres.statistics.generated.DiagramStatistic;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.DiagramMetric.DiagramMetricComponent;
import wres.statistics.generated.DiagramMetric.DiagramMetricComponent.DiagramComponentName;
import wres.statistics.generated.DiagramStatistic.DiagramStatisticComponent;

/**
 * Computes the Relative Operating Characteristic (ROC; also known as the Receiver Operating Characteristic), which
 * compares the probability of detection (PoFD) against the probability of false detection (PoFD). The empirical ROC is
 * computed for a discrete number of probability thresholds or classifiers that determine whether the forecast event
 * occurred, based on the forecast probability.
 * 
 * @author james.brown@hydrosolved.com
 */

public class RelativeOperatingCharacteristicDiagram
        extends Diagram<SampleData<Pair<Probability, Probability>>, DiagramStatisticOuter>
{
    /**
     * Probability of detection.
     */

    public static final DiagramMetricComponent PROBABILITY_OF_DETECTION = DiagramMetricComponent.newBuilder()
                                                                                                .setName( DiagramComponentName.PROBABILITY_OF_DETECTION )
                                                                                                .setMinimum( 0 )
                                                                                                .setMaximum( 1 )
                                                                                                .build();

    /**
     * Probability of false detection.
     */

    public static final DiagramMetricComponent PROBABILITY_OF_FALSE_DETECTION = DiagramMetricComponent.newBuilder()
                                                                                                      .setName( DiagramComponentName.PROBABILITY_OF_FALSE_DETECTION )
                                                                                                      .setMinimum( 0 )
                                                                                                      .setMaximum( 1 )
                                                                                                      .build();

    /**
     * Basic description of the metric.
     */

    public static final DiagramMetric BASIC_METRIC = DiagramMetric.newBuilder()
                                                                  .setName( MetricName.RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM )
                                                                  .build();

    /**
     * Full description of the metric.
     */

    public static final DiagramMetric METRIC = DiagramMetric.newBuilder()
                                                            .addComponents( RelativeOperatingCharacteristicDiagram.PROBABILITY_OF_DETECTION )
                                                            .addComponents( RelativeOperatingCharacteristicDiagram.PROBABILITY_OF_FALSE_DETECTION )
                                                            .setName( MetricName.RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM )
                                                            .build();

    /**
     * Default number of points in the diagram.
     */

    private static final int DEFAULT_POINT_COUNT = 10;

    /**
     * Components of the ROC.
     */

    private final MetricCollection<SampleData<Pair<Boolean, Boolean>>, DoubleScoreStatisticOuter, DoubleScoreStatisticOuter> roc;

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

    public static RelativeOperatingCharacteristicDiagram of() throws MetricParameterException
    {
        return new RelativeOperatingCharacteristicDiagram( DEFAULT_POINT_COUNT );
    }

    @Override
    public DiagramStatisticOuter apply( final SampleData<Pair<Probability, Probability>> s )
    {
        if ( Objects.isNull( s ) )
        {
            throw new SampleDataException( "Specify non-null input to the '" + this + "'." );
        }

        //Determine the empirical ROC. 
        //For each classifier, derive the pairs of booleans and compute the PoD and PoFD from the
        //2x2 contingency table, using a metric collection to compute the table only once
        double constant = 1.0 / this.points;
        double[] pOD = new double[this.points + 1];
        double[] pOFD = new double[this.points + 1];

        // Initialize arrays
        Arrays.fill( pOD, MissingValues.DOUBLE );
        Arrays.fill( pOFD, MissingValues.DOUBLE );

        // Some data to process        
        if ( !s.getRawData().isEmpty() )
        {
            for ( int i = 1; i < this.points; i++ )
            {
                double prob = Precision.round( 1.0 - ( i * constant ), 5 );
                //Compute the PoD/PoFD using the probability threshold to determine whether the event occurred
                //according to the probability on the RHS

                // Tranformer from probabilities to yes/no
                Function<Pair<Probability, Probability>, Pair<Boolean, Boolean>> transformer =
                        in -> Pair.of( Double.compare( in.getLeft().getProbability(),
                                                       1.0 ) == 0,
                                       in.getRight().getProbability() > prob );

                // Transformed pairs
                SampleData<Pair<Boolean, Boolean>> transformed = Slicer.transform( s, transformer );
                List<DoubleScoreStatisticOuter> out = this.roc.apply( transformed );

                //Store
                pOD[i] = Slicer.filter( out, MetricConstants.PROBABILITY_OF_DETECTION )
                               .get( 0 )
                               .getComponent( MetricConstants.MAIN )
                               .getData()
                               .getValue();
                pOFD[i] = Slicer.filter( out, MetricConstants.PROBABILITY_OF_FALSE_DETECTION )
                                .get( 0 )
                                .getComponent( MetricConstants.MAIN )
                                .getData()
                                .getValue();
            }

            //Set the lower and upper margins to (0.0, 0.0) and (1.0, 1.0), respectively            
            pOD[0] = 0.0;
            pOFD[0] = 0.0;
            pOD[this.points] = 1.0;
            pOFD[this.points] = 1.0;
        }

        DiagramStatisticComponent pod =
                DiagramStatisticComponent.newBuilder()
                                         .setMetric( RelativeOperatingCharacteristicDiagram.PROBABILITY_OF_DETECTION )
                                         .addAllValues( Arrays.stream( pOD ).boxed().collect( Collectors.toList() ) )
                                         .build();

        DiagramStatisticComponent pofd =
                DiagramStatisticComponent.newBuilder()
                                         .setMetric( RelativeOperatingCharacteristicDiagram.PROBABILITY_OF_FALSE_DETECTION )
                                         .addAllValues( Arrays.stream( pOFD ).boxed().collect( Collectors.toList() ) )
                                         .build();

        DiagramStatistic rocDiagram = DiagramStatistic.newBuilder()
                                                      .addStatistics( pod )
                                                      .addStatistics( pofd )
                                                      .setMetric( RelativeOperatingCharacteristicDiagram.BASIC_METRIC )
                                                      .build();

        return DiagramStatisticOuter.of( rocDiagram, s.getMetadata() );
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
            throws MetricParameterException
    {
        super();
        this.roc = MetricFactory.ofDichotomousScoreCollection( MetricConstants.PROBABILITY_OF_DETECTION,
                                                               MetricConstants.PROBABILITY_OF_FALSE_DETECTION );
        //Set the default points
        this.points = points;
    }

}
