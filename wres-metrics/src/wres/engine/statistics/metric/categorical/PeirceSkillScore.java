package wres.engine.statistics.metric.categorical;

import java.util.Iterator;
import java.util.Objects;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.MetricConstants;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.datamodel.statistics.StatisticMetadata;
import wres.engine.statistics.metric.FunctionFactory;
import wres.engine.statistics.metric.MetricCalculationException;
import wres.statistics.generated.DoubleScoreMetric;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent.ComponentName;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;

/**
 * <P>The Peirce Skill Score is a categorical measure of the average accuracy of a predictand for a multi-category event,
 * where the climatological probability of identifying the correct category is factored out. For a dichotomous
 * predictand, the Peirce Skill Score corresponds to the difference between the {@link ProbabilityOfDetection} and the
 * {@link ProbabilityOfFalseDetection}. 
 * 
 * <p>TODO: The {@link #aggregate(DoubleScoreStatisticOuter)} is implemented for the multicategory case. Abstract this to 
 * somewhere appropriately visible for extensibility. 
 * 
 * @author james.brown@hydrosolved.com
 */
public class PeirceSkillScore extends ContingencyTableScore
{

    /**
     * Canonical description of the metric.
     */

    public static final DoubleScoreMetric METRIC =
            DoubleScoreMetric.newBuilder()
                             .addComponents( DoubleScoreMetricComponent.newBuilder()
                                                                       .setMinimum( -1 )
                                                                       .setMaximum( 1 )
                                                                       .setOptimum( 1 )
                                                                       .setName( ComponentName.MAIN ) )
                             .setName( MetricName.PEIRCE_SKILL_SCORE )
                             .build();

    /**
     * Returns an instance.
     * 
     * @return an instance
     */

    public static PeirceSkillScore of()
    {
        return new PeirceSkillScore();
    }

    @Override
    public DoubleScoreStatisticOuter apply( final SampleData<Pair<Boolean, Boolean>> s )
    {
        return aggregate( this.getInputForAggregation( s ) );
    }

    @Override
    public DoubleScoreStatisticOuter aggregate( final DoubleScoreStatisticOuter output )
    {
        if ( Objects.isNull( output ) )
        {
            throw new SampleDataException( "Specify non-null input to the '" + this.toString() + "'." );
        }

        if ( output.getComponents().size() == 4 )
        {
            return this.aggregateTwoByTwo( output );
        }

        return this.aggregateNByN( output );
    }

    @Override
    public MetricConstants getID()
    {
        return MetricConstants.PEIRCE_SKILL_SCORE;
    }

    @Override
    public boolean isSkillScore()
    {
        return true;
    }

    /**
     * Computes the score for the 2x2 contingency table.
     * 
     * @param contingencyTable the 2x2 contingency table components
     * @return the score
     */

    private DoubleScoreStatisticOuter aggregateTwoByTwo( DoubleScoreStatisticOuter contingencyTable )
    {
        this.is2x2ContingencyTable( contingencyTable, this );

        double tP = contingencyTable.getComponent( MetricConstants.TRUE_POSITIVES )
                                    .getData()
                                    .getValue();

        double fP = contingencyTable.getComponent( MetricConstants.FALSE_POSITIVES )
                                    .getData()
                                    .getValue();

        double fN = contingencyTable.getComponent( MetricConstants.FALSE_NEGATIVES )
                                    .getData()
                                    .getValue();

        double tN = contingencyTable.getComponent( MetricConstants.TRUE_NEGATIVES )
                                    .getData()
                                    .getValue();

        StatisticMetadata meta = this.getMetadata( contingencyTable );

        double result = FunctionFactory.finiteOrMissing()
                                       .applyAsDouble( ( tP / ( tP + fN ) )
                                                       - ( fP / ( fP + tN ) ) );

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setName( ComponentName.MAIN )
                                                                               .setValue( result )
                                                                               .build();
        DoubleScoreStatistic score =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( PeirceSkillScore.METRIC )
                                    .addStatistics( component )
                                    .build();

        return DoubleScoreStatisticOuter.of( score, meta );
    }

    /**
     * Computes the score for the NxN contingency table. The elements must be ordered from top-left to bottom right,
     * based on the mapped dimension name.
     * 
     * @param contingencyTable the NxN contingency table components
     * @return the score
     */

    private DoubleScoreStatisticOuter aggregateNByN( DoubleScoreStatisticOuter contingencyTable )
    {
        //Check the input
        this.isContingencyTable( contingencyTable, this );

        int square = (int) Math.sqrt( contingencyTable.getComponents().size() );

        double[][] cm = new double[square][square];

        Iterator<MetricConstants> source = contingencyTable.getComponents().iterator();
        for ( int i = 0; i < square; i++ )
        {
            for ( int j = 0; j < square; j++ )
            {
                cm[i][j] = contingencyTable.getComponent( source.next() )
                                           .getData()
                                           .getValue();
            }
        }

        double diag = 0.0; //Sum of diagonal
        final double[] rowSums = new double[cm.length]; //Row sums
        final double[] colSums = new double[cm.length]; //Col sums
        for ( int i = 0; i < cm.length; i++ )
        {
            diag += cm[i][i];
            for ( int j = 0; j < cm.length; j++ )
            {
                rowSums[i] += cm[i][j];
                colSums[j] += cm[i][j];
            }
        }
        //Compute the sum product terms
        double sumProd = 0.0;
        double uniProd = 0.0;
        double n = 0.0;
        for ( int i = 0; i < cm.length; i++ )
        {
            sumProd += ( rowSums[i] * colSums[i] );
            uniProd += ( colSums[i] * colSums[i] );
            n += rowSums[i];
        }
        if ( n <= 0 )
        {
            throw new MetricCalculationException( "The sum product of the rows and columns in the contingency table "
                                                  + "must exceed zero when computing the '"
                                                  + this
                                                  + "': "
                                                  + n );
        }
        //Compose the result
        final double nSquared = n * n;
        final double result = FunctionFactory.finiteOrMissing()
                                             .applyAsDouble( ( ( diag / n ) - ( sumProd / nSquared ) )
                                                             / ( 1.0 - ( uniProd / nSquared ) ) );

        StatisticMetadata meta = this.getMetadata( contingencyTable );

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setName( ComponentName.MAIN )
                                                                               .setValue( result )
                                                                               .build();
        DoubleScoreStatistic score =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( PeirceSkillScore.METRIC )
                                    .addStatistics( component )
                                    .build();

        return DoubleScoreStatisticOuter.of( score, meta );
    }


    /**
     * Hidden constructor.
     */

    private PeirceSkillScore()
    {
        super();
    }

}
