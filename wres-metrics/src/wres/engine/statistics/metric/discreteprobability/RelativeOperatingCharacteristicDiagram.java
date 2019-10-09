package wres.engine.statistics.metric.discreteprobability;

import java.util.Arrays;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.math3.util.Precision;

import wres.datamodel.MetricConstants;
import wres.datamodel.Probability;
import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.MissingValues;
import wres.datamodel.Slicer;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.statistics.DoubleScoreStatistic;
import wres.datamodel.statistics.ListOfStatistics;
import wres.datamodel.statistics.MatrixStatistic;
import wres.datamodel.statistics.DiagramStatistic;
import wres.datamodel.statistics.StatisticMetadata;
import wres.engine.statistics.metric.Diagram;
import wres.engine.statistics.metric.MetricCollection;
import wres.engine.statistics.metric.MetricFactory;
import wres.engine.statistics.metric.MetricParameterException;

/**
 * Computes the Relative Operating Characteristic (ROC; also known as the Receiver Operating Characteristic), which
 * compares the probability of detection (PoFD) against the probability of false detection (PoFD). The empirical ROC is
 * computed for a discrete number of probability thresholds or classifiers that determine whether the forecast event
 * occurred, based on the forecast probability.
 * 
 * @author james.brown@hydrosolved.com
 */

public class RelativeOperatingCharacteristicDiagram
        extends Diagram<SampleData<Pair<Probability, Probability>>, DiagramStatistic>
{

    /**
     * Default number of points in the diagram.
     */

    private static final int DEFAULT_POINT_COUNT = 10;

    /**
     * Components of the ROC.
     */

    private final MetricCollection<SampleData<Pair<Boolean, Boolean>>, MatrixStatistic, DoubleScoreStatistic> roc;

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
    public DiagramStatistic apply( final SampleData<Pair<Probability, Probability>> s )
    {
        if ( Objects.isNull( s ) )
        {
            throw new SampleDataException( "Specify non-null input to the '" + this + "'." );
        }
        //Determine the empirical ROC. 
        //For each classifier, derive the pairs of booleans and compute the PoD and PoFD from the
        //2x2 contingency table, using a metric collection to compute the table only once
        double constant = 1.0 / points;
        double[] pOD = new double[points + 1];
        double[] pOFD = new double[points + 1];

        // Initialize arrays
        Arrays.fill( pOD, MissingValues.DOUBLE );
        Arrays.fill( pOFD, MissingValues.DOUBLE );

        // Some data to process        
        if ( !s.getRawData().isEmpty() )
        {
            for ( int i = 1; i < points; i++ )
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
                ListOfStatistics<DoubleScoreStatistic> out =
                        roc.apply( transformed );
                //Store
                pOD[i] = Slicer.filter( out, MetricConstants.PROBABILITY_OF_DETECTION ).getData().get( 0 ).getData();
                pOFD[i] = Slicer.filter( out, MetricConstants.PROBABILITY_OF_FALSE_DETECTION )
                                .getData()
                                .get( 0 )
                                .getData();
            }

            //Set the lower and upper margins to (0.0, 0.0) and (1.0, 1.0), respectively            
            pOD[0] = 0.0;
            pOFD[0] = 0.0;
            pOD[points] = 1.0;
            pOFD[points] = 1.0;
        }

        //Set the results
        Map<MetricDimension, VectorOfDoubles> output = new EnumMap<>( MetricDimension.class );
        output.put( MetricDimension.PROBABILITY_OF_DETECTION, VectorOfDoubles.of( pOD ) );
        output.put( MetricDimension.PROBABILITY_OF_FALSE_DETECTION, VectorOfDoubles.of( pOFD ) );
        final StatisticMetadata metOut =
                StatisticMetadata.of( s.getMetadata(),
                                      this.getID(),
                                      MetricConstants.MAIN,
                                      this.hasRealUnits(),
                                      s.getRawData().size(),
                                      null );
        return DiagramStatistic.of( output, metOut );
    }

    @Override
    public MetricConstants getID()
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
        roc = MetricFactory.ofDichotomousScoreCollection( MetricConstants.PROBABILITY_OF_DETECTION,
                                                          MetricConstants.PROBABILITY_OF_FALSE_DETECTION );
        //Set the default points
        this.points = points;
    }

}
