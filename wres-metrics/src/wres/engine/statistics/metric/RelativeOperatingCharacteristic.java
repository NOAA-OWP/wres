package wres.engine.statistics.metric;

import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.math3.util.Precision;

import wres.datamodel.DataFactory;
import wres.datamodel.DichotomousPairs;
import wres.datamodel.DiscreteProbabilityPairs;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.MetricInputException;
import wres.datamodel.MetricOutput;
import wres.datamodel.MetricOutputMapByMetric;
import wres.datamodel.MetricOutputMetadata;
import wres.datamodel.MultiVectorOutput;
import wres.datamodel.ScalarOutput;
import wres.datamodel.Slicer;

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

abstract class RelativeOperatingCharacteristic<T extends MetricOutput<?>>
        extends
        Metric<DiscreteProbabilityPairs, T>
{

    /**
     * Components of the ROC.
     */

    private final MetricCollection<DichotomousPairs, ScalarOutput> roc;

    /**
     * Returns the components of the Relative Operating Characteristic for a prescribed number of thresholds. The
     * thresholds are used to divide the unit interval equally. A binary classifier is derived from each threshold and
     * used to classify the observed and forecast probabilities of a discrete event according to whether the threshold
     * is exceeded. Each classifier produces a pair of MetricConstants.PROBABILITY_OF_DETECTION (PoD) and
     * MetricConstants.PROBABILITY_OF_FALSE_DETECTION (PoFD), which are returned in the result.
     * 
     * @param s the pairs
     * @param points the number of thresholds
     * @return a {@link MultiVectorOutput} containing the pairs of PoD and PoFD
     */

    MultiVectorOutput getROC( final DiscreteProbabilityPairs s, int points )
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
    public boolean hasRealUnits()
    {
        return false;
    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    RelativeOperatingCharacteristic( final MetricBuilder<DiscreteProbabilityPairs, T> builder )
            throws MetricParameterException
    {
        super( builder );
        roc = MetricFactory.getInstance( builder.dataFactory )
                           .ofDichotomousScalarCollection( MetricConstants.PROBABILITY_OF_DETECTION,
                                                           MetricConstants.PROBABILITY_OF_FALSE_DETECTION );
    }
}
