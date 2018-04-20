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
import wres.datamodel.inputs.pairs.PairOfDoubles;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.outputs.MultiVectorOutput;
import wres.engine.statistics.metric.Diagram;
import wres.engine.statistics.metric.MetricParameterException;

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
 * @version 0.1
 * @since 0.1
 */

public class ReliabilityDiagram extends Diagram<DiscreteProbabilityPairs, MultiVectorOutput>
{

    /**
     * Number of bins in the Reliability Diagram.
     */

    private final int bins;

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
            //Compute the average probabilities for samples > 0
            s.getRawData().forEach( this.getIncrementor( fProb, oProb, samples, constant ) );
            List<Double> fProbFinal = new ArrayList<>(); //Forecast probs for samples > 0
            List<Double> oProbFinal = new ArrayList<>(); //Observed probs for samples > 0
            for ( int i = 0; i < bins; i++ )
            {
                if ( samples[i] > 0 )
                {
                    fProbFinal.add( fProb[i] / samples[i] );
                    oProbFinal.add( oProb[i] / samples[i] );
                }
            }
            fProb = fProbFinal.stream().mapToDouble( Double::doubleValue ).toArray();
            oProb = oProbFinal.stream().mapToDouble( Double::doubleValue ).toArray();
        }
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
        final MetricOutputMetadata metOut = getMetadata( s, s.getRawData().size(), MetricConstants.MAIN, null );
        return getDataFactory().ofMultiVectorOutput( output, metOut );
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
     * A {@link MetricBuilder} to build the metric.
     */

    public static class ReliabilityDiagramBuilder extends DiagramBuilder<DiscreteProbabilityPairs, MultiVectorOutput>
    {

        @Override
        public ReliabilityDiagram build() throws MetricParameterException
        {
            return new ReliabilityDiagram( this );
        }

    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     * @throws MetricParameterException if one or more parameters is invalid 
     */

    protected ReliabilityDiagram( final ReliabilityDiagramBuilder builder ) throws MetricParameterException
    {
        super( builder );
        //Set the default bins
        bins = 10;
    }

    /**
     * Consumer that increments the input probabilities and sample sizes.
     * 
     * @param fProb the forecast probabilities to increment
     * @param oProb the observed relative frequencies to increment
     * @param constant the fraction occupied by each bin
     */

    private Consumer<PairOfDoubles>
            getIncrementor( final double[] fProb, final double[] oProb, final double[] samples, final double constant )
    {
        //Consumer that increments the probabilities and sample size
        return pair -> {
            
            //Determine forecast bin
            for ( int i = 0; i < bins; i++ )
            {
                //Define the bin
                double lower = Precision.round( i * constant, 5 );
                double upper = Precision.round( lower + constant, 5 );
                if ( i == 0 )
                {
                    lower = -1.0; //Catch forecast probabilities of zero in the first bin
                }
                //Establish whether the forecast probability falls inside it
                if ( pair.getItemTwo() > lower && pair.getItemTwo() <= upper )
                {
                    fProb[i] += pair.getItemTwo();
                    oProb[i] += pair.getItemOne();
                    samples[i] += 1;
                    break;
                }
            }
        };
    }


}
