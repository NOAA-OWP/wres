package wres.engine.statistics.metric.discreteprobability;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.math3.util.Precision;

import wres.datamodel.MetricConstants;
import wres.datamodel.Probability;
import wres.datamodel.MissingValues;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.statistics.DiagramStatisticOuter;
import wres.engine.statistics.metric.Diagram;
import wres.statistics.generated.DiagramMetric;
import wres.statistics.generated.DiagramStatistic;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.DiagramMetric.DiagramMetricComponent;
import wres.statistics.generated.DiagramMetric.DiagramMetricComponent.DiagramComponentName;
import wres.statistics.generated.DiagramStatistic.DiagramStatisticComponent;

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

public class ReliabilityDiagram extends Diagram<SampleData<Pair<Probability, Probability>>, DiagramStatisticOuter>
{

    /**
     * Forecast probability.
     */

    public static final DiagramMetricComponent FORECAST_PROBABILITY = DiagramMetricComponent.newBuilder()
                                                                                            .setName( DiagramComponentName.FORECAST_PROBABILITY )
                                                                                            .setMinimum( 0 )
                                                                                            .setMaximum( 1 )
                                                                                            .setUnits( "PROBABILITY" )
                                                                                            .build();

    /**
     * Observed frequency.
     */

    public static final DiagramMetricComponent OBSERVED_RELATIVE_FREQUENCY = DiagramMetricComponent.newBuilder()
                                                                                                   .setName( DiagramComponentName.OBSERVED_RELATIVE_FREQUENCY )
                                                                                                   .setMinimum( 0 )
                                                                                                   .setMaximum( 1 )
                                                                                                   .setUnits( "PROBABILITY" )
                                                                                                   .build();

    /**
     * Sample size or sharpness.
     */

    public static final DiagramMetricComponent SAMPLE_SIZE = DiagramMetricComponent.newBuilder()
                                                                                   .setName( DiagramComponentName.SAMPLE_SIZE )
                                                                                   .setMinimum( 0 )
                                                                                   .setMaximum( Double.POSITIVE_INFINITY )
                                                                                   .setUnits( "COUNT" )
                                                                                   .build();

    /**
     * Basic description of the metric.
     */

    public static final DiagramMetric BASIC_METRIC = DiagramMetric.newBuilder()
                                                                  .setName( MetricName.RELIABILITY_DIAGRAM )
                                                                  .build();

    /**
     * Full description of the metric.
     */

    public static final DiagramMetric METRIC = DiagramMetric.newBuilder()
                                                            .setName( MetricName.RELIABILITY_DIAGRAM )
                                                            .build();

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
    public DiagramStatisticOuter apply( final SampleData<Pair<Probability, Probability>> s )
    {
        if ( Objects.isNull( s ) )
        {
            throw new SampleDataException( "Specify non-null input to the '" + this + "'." );
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
                    fProbFinal.add( MissingValues.DOUBLE );
                    oProbFinal.add( MissingValues.DOUBLE );
                }
            }

            // Stream to an array
            fProb = fProbFinal.stream().mapToDouble( Double::doubleValue ).toArray();
            oProb = oProbFinal.stream().mapToDouble( Double::doubleValue ).toArray();
        }
        // No data available
        else
        {
            Arrays.fill( fProb, MissingValues.DOUBLE );
            Arrays.fill( oProb, MissingValues.DOUBLE );
        }

        DiagramStatisticComponent forecastProbability =
                DiagramStatisticComponent.newBuilder()
                                         .setMetric( ReliabilityDiagram.FORECAST_PROBABILITY )
                                         .addAllValues( Arrays.stream( fProb )
                                                              .boxed()
                                                              .collect( Collectors.toList() ) )
                                         .build();

        DiagramStatisticComponent observedFrequency =
                DiagramStatisticComponent.newBuilder()
                                         .setMetric( ReliabilityDiagram.OBSERVED_RELATIVE_FREQUENCY )
                                         .addAllValues( Arrays.stream( oProb )
                                                              .boxed()
                                                              .collect( Collectors.toList() ) )
                                         .build();

        DiagramStatisticComponent sampleSize =
                DiagramStatisticComponent.newBuilder()
                                         .setMetric( ReliabilityDiagram.SAMPLE_SIZE )
                                         .addAllValues( Arrays.stream( samples )
                                                              .boxed()
                                                              .collect( Collectors.toList() ) )
                                         .build();

        DiagramStatistic statistic = DiagramStatistic.newBuilder()
                                                     .addStatistics( forecastProbability )
                                                     .addStatistics( observedFrequency )
                                                     .addStatistics( sampleSize )
                                                     .setMetric( ReliabilityDiagram.BASIC_METRIC )
                                                     .build();

        return DiagramStatisticOuter.of( statistic, s.getMetadata() );
    }

    @Override
    public MetricConstants getMetricName()
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

    private Consumer<Pair<Probability, Probability>>
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
                if ( pair.getRight().getProbability() > lower && pair.getRight().getProbability() <= upper )
                {
                    fProb[i] += pair.getRight().getProbability();
                    oProb[i] += pair.getLeft().getProbability();
                    samples[i] += 1;
                    break;
                }
            }
        };
    }


}
