package wres.engine.statistics.metric.discreteprobability;

import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.math3.util.Precision;

import wres.datamodel.DataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.Slicer;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.DichotomousPairs;
import wres.datamodel.inputs.pairs.DiscreteProbabilityPairs;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.outputs.MatrixOutput;
import wres.datamodel.outputs.MetricOutputMapByMetric;
import wres.datamodel.outputs.MultiVectorOutput;
import wres.datamodel.outputs.ScalarOutput;
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
 * @version 0.1
 * @since 0.1
 */

public class RelativeOperatingCharacteristicDiagram extends Diagram<DiscreteProbabilityPairs, MultiVectorOutput>
{

    /**
     * Components of the ROC.
     */

    private final MetricCollection<DichotomousPairs, MatrixOutput, ScalarOutput> roc;    
    
    /**
     * Number of points in the empirical ROC diagram.
     */

    private final int points;

    @Override
    public MultiVectorOutput apply( final DiscreteProbabilityPairs s )
    {
        if ( Objects.isNull( s ) )
        {
            throw new MetricInputException( "Specify non-null input to the '" + this + "'." );
        }
        //Determine the empirical ROC. 
        //For each classifier, derive the pairs of booleans and compute the PoD and PoFD from the
        //2x2 contingency table, using a metric collection to compute the table only once
        double constant = 1.0 / points;
        double[] pOD = new double[points + 1];
        double[] pOFD = new double[points + 1];
        DataFactory d = getDataFactory();
        Slicer slice = d.getSlicer();

        for ( int i = 1; i < points; i++ )
        {
            double prob = Precision.round( 1.0 - ( i * constant ), 5 );
            //Compute the PoD/PoFD using the probability threshold to determine whether the event occurred
            //according to the probability on the RHS
            MetricOutputMapByMetric<ScalarOutput> out =
                    roc.apply( slice.transformPairs( s,
                                                     in -> d.pairOf( Double.compare( in.getItemOne(),
                                                                                     1.0 ) == 0,
                                                                     in.getItemTwo() > prob ) ) );
            //Store
            pOD[i] = out.get( MetricConstants.PROBABILITY_OF_DETECTION ).getData();
            pOFD[i] = out.get( MetricConstants.PROBABILITY_OF_FALSE_DETECTION ).getData();
        }
        //Set the upper point to (1.0, 1.0)
        pOD[points] = 1.0;
        pOFD[points] = 1.0;

        //Set the results
        Map<MetricDimension, double[]> output = new EnumMap<>( MetricDimension.class );
        output.put( MetricDimension.PROBABILITY_OF_DETECTION, pOD );
        output.put( MetricDimension.PROBABILITY_OF_FALSE_DETECTION, pOFD );
        final MetricOutputMetadata metOut = getMetadata( s, s.getData().size(), MetricConstants.MAIN, null );
        return d.ofMultiVectorOutput( output, metOut );
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
     * A {@link MetricBuilder} to build the metric.
     */

    public static class RelativeOperatingCharacteristicBuilder
            extends
            DiagramBuilder<DiscreteProbabilityPairs, MultiVectorOutput>
    {

        @Override
        public RelativeOperatingCharacteristicDiagram build() throws MetricParameterException
        {
            return new RelativeOperatingCharacteristicDiagram( this );
        }

    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    private RelativeOperatingCharacteristicDiagram( final RelativeOperatingCharacteristicBuilder builder )
            throws MetricParameterException
    {
        super( builder );
        roc = MetricFactory.getInstance( getDataFactory() )
                .ofDichotomousScalarCollection( MetricConstants.PROBABILITY_OF_DETECTION,
                                                MetricConstants.PROBABILITY_OF_FALSE_DETECTION );
        //Set the default points
        points = 10;
    }

}
