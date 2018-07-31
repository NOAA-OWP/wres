package wres.engine.statistics.metric.discreteprobability;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

import org.apache.commons.math3.util.Precision;

import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.MetricConstants.MissingValues;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.DiscreteProbabilityPairs;
import wres.datamodel.inputs.pairs.SingleValuedPair;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.outputs.MultiVectorOutput;
import wres.engine.statistics.metric.Diagram;

/**
 * <p>
 * Computes the Reliability Diagram, which compares the forecast probability of a discrete event against the conditional
 * observed probability, given the forecast probability. Using the sample data, the forecast probabilities are collected
 * into a series of bins that map the unit interval, [0,1]. The corresponding dichotomous observations are pooled and
 * the conditional observed probability is obtained from the average of the pooled observations within each bin. The
 * forecast probability is also obtained from the average of the pooled forecast probabilities within each bin.
 * </p>
 * <p>
 * The Reliability Diagram comprises the average forecast probabilities, the average conditional observed probabilities,
 * and the sample sizes (sharpness) for each bin within the unit interval.
 * </p>
 * 
 * @author james.brown@hydrosolved.com
 */

public class ReliabilityDiagram extends Diagram<DiscreteProbabilityPairs, MultiVectorOutput>
{

    /**
     * Default number of bins.
     */
    
    private static final int DEFAULT_BIN_COUNT = 10;
    
    /**
     * Number of bins in the Reliability Diagram.
     */

    private final int bins;

    /**
     * Returns an instance.
     * 
     * @return an instance
     */
    
    public static ReliabilityDiagram of()
    {
        return new ReliabilityDiagram( DEFAULT_BIN_COUNT );
    }  
    
    @Override
    public MultiVectorOutput apply( final DiscreteProbabilityPairs s )
    {
        if ( Objects.isNull( s ) )
        {
            throw new MetricInputException( "Specify non-null input to the '" + this + "'." );
        }
        // Determine the probabilities and sample sizes 
        double constant = 1.0 / bins;
        double[] fProb = new double[bins];
        double[] oProb = new double[bins];
        double[] samples = new double[bins];

        // Some data available
        if ( !s.getRawData().isEmpty() )
        {
            // Compute the average probabilities for samples > 0

            // Increment the reliability 
            s.getRawData().forEach( this.getIncrementor( fProb, oProb, samples, constant ) );

            // Compute the average reliability
            List<Double> fProbFinal = new ArrayList<>(); //Forecast probs for samples > 0
            List<Double> oProbFinal = new ArrayList<>(); //Observed probs for samples > 0
            for ( int i = 0; i < bins; i++ )
            {
                // Bin with > 0 samples
                if ( samples[i] > 0 )
                {
                    fProbFinal.add( fProb[i] / samples[i] );
                    oProbFinal.add( oProb[i] / samples[i] );
                }
                // Bin with no samples
                else
                {
                    fProbFinal.add( MissingValues.MISSING_DOUBLE );
                    oProbFinal.add( MissingValues.MISSING_DOUBLE );
                }
            }

            // Stream to an array
            fProb = fProbFinal.stream().mapToDouble( Double::doubleValue ).toArray();
            oProb = oProbFinal.stream().mapToDouble( Double::doubleValue ).toArray();
        }
        // No data available
        else
        {
            Arrays.fill( fProb, MissingValues.MISSING_DOUBLE );
            Arrays.fill( oProb, MissingValues.MISSING_DOUBLE );
        }

        // Set the results
        Map<MetricDimension, double[]> output = new EnumMap<>( MetricDimension.class );
        output.put( MetricDimension.FORECAST_PROBABILITY, fProb );
        output.put( MetricDimension.OBSERVED_RELATIVE_FREQUENCY, oProb );
        output.put( MetricDimension.SAMPLE_SIZE, samples );

        MetricOutputMetadata metOut = getMetadata( s, s.getRawData().size(), MetricConstants.MAIN, null );

        return MultiVectorOutput.ofMultiVectorOutput( output, metOut );
    }

    @Override
    public MetricConstants getID()
    {
        return MetricConstants.RELIABILITY_DIAGRAM;
    }

    @Override
    public boolean hasRealUnits()
    {
        return false;
    }

    /**
     * Hidden constructor.
     * 
     * @param bins the number of bins in the diagram
     */

    protected ReliabilityDiagram( int bins )
    {
        super();

        this.bins = bins;
    }

    /**
     * Consumer that increments the input probabilities and sample sizes.
     * 
     * @param fProb the forecast probabilities to increment
     * @param oProb the observed relative frequencies to increment
     * @param constant the fraction occupied by each bin
     */

    private Consumer<SingleValuedPair>
            getIncrementor( final double[] fProb, final double[] oProb, final double[] samples, final double constant )
    {
        //Consumer that increments the probabilities and sample size
        return pair -> {

            //Determine forecast bin
            for ( int i = 0; i < this.bins; i++ )
            {
                //Define the bin
                double lower = Precision.round( i * constant, 5 );
                double upper = Precision.round( lower + constant, 5 );
                if ( i == 0 )
                {
                    lower = -1.0; //Catch forecast probabilities of zero in the first bin
                }
                //Establish whether the forecast probability falls inside it
                if ( pair.getRight() > lower && pair.getRight() <= upper )
                {
                    fProb[i] += pair.getRight();
                    oProb[i] += pair.getLeft();
                    samples[i] += 1;
                    break;
                }
            }
        };
    }


}
