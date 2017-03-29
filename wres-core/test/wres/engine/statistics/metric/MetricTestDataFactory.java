package wres.engine.statistics.metric;

import wres.engine.statistics.metric.inputs.DichotomousPairs;
import wres.engine.statistics.metric.inputs.DiscreteProbabilityPairs;
import wres.engine.statistics.metric.inputs.SingleValuedPairs;

/**
 * Factory class for generating test datasets for metric calculations.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class MetricTestDataFactory
{

    /**
     * Returns a set of single-valued pairs without dimension,
     * 
     * @return single-valued pairs with no dimension
     */

    public static SingleValuedPairs getSingleValuedPairsOne()
    {
        //Construct some single-valued pairs
        final double[][] dData = new double[][]{{22.9, 22.8}, {75.2, 80}, {63.2, 65}, {29, 30}, {5, 2}, {2.1, 3.1},
            {35000, 37000}, {8, 7}, {12, 12}, {93, 94}};
        return new SingleValuedPairs(dData, null);
    }

    /**
     * Returns a set of dichotomous pairs based on http://www.cawcr.gov.au/projects/verification/#Contingency_table. The
     * test data comprises 83 hits, 38 false alarms, 23 misses and 222 correct negatives, i.e. N=365.
     * 
     * @return a set of dichotomous pairs
     */

    public static DichotomousPairs getDichotomousPairsOne()
    {
        //Construct the dichotomous pairs using the example from http://www.cawcr.gov.au/projects/verification/#Contingency_table
        //83 hits, 38 false alarms, 23 misses and 222 correct negatives, i.e. N=365
        final boolean[][] bData = new boolean[365][2];
        //Hits
        for(int i = 0; i < 82; i++)
        {
            bData[i][0] = bData[i][1] = true;
        }
        //False alarms
        for(int i = 82; i < 120; i++)
        {
            bData[i][1] = true;
        }
        //Misses
        for(int i = 120; i < 143; i++)
        {
            bData[i][0] = true;
        }
        return new DichotomousPairs(bData); //Construct the pairs
    }

    /**
     * Returns a set of discrete probability pair.
     * 
     * @return discrete probability pairs
     */

    public static DiscreteProbabilityPairs getDiscreteProbabilityPairsOne()
    {
        //Construct some probabilistic pairs, and use the same pairs as a reference for skill (i.e. skill = 0.0)
        final double[][] pData = new double[][]{{0, 3.0 / 5.0}, {0, 1.0 / 5.0}, {1, 2.0 / 5.0}, {1, 3.0 / 5.0},
            {0, 0.0 / 5.0}, {1, 1.0 / 5.0}};
        return new DiscreteProbabilityPairs(pData, pData);
    }

}
