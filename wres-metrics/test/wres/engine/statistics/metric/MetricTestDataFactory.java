package wres.engine.statistics.metric;

import java.util.ArrayList;
import java.util.List;

import wres.datamodel.PairOfDoubles;
import wres.datamodel.VectorOfBooleans;
import wres.datamodel.metric.DataFactory;
import wres.datamodel.metric.DefaultDataFactory;
import wres.datamodel.metric.DichotomousPairs;
import wres.datamodel.metric.DiscreteProbabilityPairs;
import wres.datamodel.metric.Metadata;
import wres.datamodel.metric.MetadataFactory;
import wres.datamodel.metric.MulticategoryPairs;
import wres.datamodel.metric.SingleValuedPairs;

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
     * Returns a set of single-valued pairs without a baseline.
     * 
     * @return single-valued pairs
     */

    public static SingleValuedPairs getSingleValuedPairsOne()
    {
        //Construct some single-valued pairs
        final DataFactory metIn = DefaultDataFactory.getInstance();
        final List<PairOfDoubles> values = new ArrayList<>();
        values.add(metIn.pairOf(22.9, 22.8));
        values.add(metIn.pairOf(75.2, 80));
        values.add(metIn.pairOf(63.2, 65));
        values.add(metIn.pairOf(29, 30));
        values.add(metIn.pairOf(5, 2));
        values.add(metIn.pairOf(2.1, 3.1));
        values.add(metIn.pairOf(35000, 37000));
        values.add(metIn.pairOf(8, 7));
        values.add(metIn.pairOf(12, 12));
        values.add(metIn.pairOf(93, 94));
        final MetadataFactory metFac = metIn.getMetadataFactory();
        return metIn.ofSingleValuedPairs(values, metFac.getMetadata(values.size()));
    }

    /**
     * Returns a set of single-valued pairs with a baseline.
     * 
     * @return single-valued pairs
     */

    public static SingleValuedPairs getSingleValuedPairsTwo()
    {
        //Construct some single-valued pairs
        final DataFactory metIn = DefaultDataFactory.getInstance();
        final List<PairOfDoubles> values = new ArrayList<>();
        values.add(metIn.pairOf(22.9, 22.8));
        values.add(metIn.pairOf(75.2, 80));
        values.add(metIn.pairOf(63.2, 65));
        values.add(metIn.pairOf(29, 30));
        values.add(metIn.pairOf(5, 2));
        values.add(metIn.pairOf(2.1, 3.1));
        values.add(metIn.pairOf(35000, 37000));
        values.add(metIn.pairOf(8, 7));
        values.add(metIn.pairOf(12, 12));
        values.add(metIn.pairOf(93, 94));
        final List<PairOfDoubles> baseline = new ArrayList<>();
        baseline.add(metIn.pairOf(20.9, 23.8));
        baseline.add(metIn.pairOf(71.2, 83.2));
        baseline.add(metIn.pairOf(69.2, 66));
        baseline.add(metIn.pairOf(20, 30.5));
        baseline.add(metIn.pairOf(5.8, 2.1));
        baseline.add(metIn.pairOf(1.1, 3.4));
        baseline.add(metIn.pairOf(33020, 37500));
        baseline.add(metIn.pairOf(8.8, 7.1));
        baseline.add(metIn.pairOf(12.1, 13));
        baseline.add(metIn.pairOf(93.2, 94.8));
        final MetadataFactory metFac = metIn.getMetadataFactory();
        final Metadata main = metFac.getMetadata(values.size(),
                                                 metFac.getDimension("CMS"),
                                                 metFac.getDatasetIdentifier("DRRC2", "SQIN", "HEFS"));
        final Metadata base = metFac.getMetadata(values.size(),
                                                 metFac.getDimension("CMS"),
                                                 metFac.getDatasetIdentifier("DRRC2", "SQIN", "ESP"));
        return metIn.ofSingleValuedPairs(values, baseline, main, base);
    }

    /**
     * Returns a moderately-sized (10k) test dataset of single-valued pairs, {5,10}, without a baseline.
     * 
     * @return single-valued pairs
     */

    public static SingleValuedPairs getSingleValuedPairsThree()
    {
        //Construct some single-valued pairs
        final List<PairOfDoubles> values = new ArrayList<>();
        final DataFactory metIn = DefaultDataFactory.getInstance();
        for(int i = 0; i < 10000; i++)
        {
            values.add(metIn.pairOf(5, 10));
        }
        final MetadataFactory metFac = metIn.getMetadataFactory();
        final Metadata meta = metFac.getMetadata(values.size(),
                                                 metFac.getDimension("CMS"),
                                                 metFac.getDatasetIdentifier("DRRC2", "SQIN", "HEFS"));
        return metIn.ofSingleValuedPairs(values, meta);
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
        final DataFactory metIn = DefaultDataFactory.getInstance();
        final List<VectorOfBooleans> values = new ArrayList<>();
        //Hits
        for(int i = 0; i < 82; i++)
        {
            values.add(metIn.vectorOf(new boolean[]{true, true}));
        }
        //False alarms
        for(int i = 82; i < 120; i++)
        {
            values.add(metIn.vectorOf(new boolean[]{false, true}));
        }
        //Misses
        for(int i = 120; i < 143; i++)
        {
            values.add(metIn.vectorOf(new boolean[]{true, false}));
        }
        for(int i = 144; i < 366; i++)
        {
            values.add(metIn.vectorOf(new boolean[]{false, false}));
        }
        final MetadataFactory metFac = metIn.getMetadataFactory();
        final Metadata meta = metFac.getMetadata(values.size(),
                                                 metFac.getDimension(),
                                                 metFac.getDatasetIdentifier("DRRC2", "SQIN", "HEFS"));
        return metIn.ofDichotomousPairs(values, meta); //Construct the pairs
    }

    /**
     * Returns a set of multicategory pairs based on Table 4.2 in Joliffe and Stephenson (2012) Forecast Verification: A
     * Practitioner's Guide in Atmospheric Science. 2nd Ed. Wiley, Chichester.
     * 
     * @return a set of dichotomous pairs
     */

    public static MulticategoryPairs getMulticategoryPairsOne()
    {
        //Construct the multicategory pairs
        final DataFactory metIn = DefaultDataFactory.getInstance();
        final List<VectorOfBooleans> values = new ArrayList<>();
        //(1,1)
        for(int i = 0; i < 24; i++)
        {
            values.add(metIn.vectorOf(new boolean[]{true, false, false, true, false, false}));
        }
        //(1,2)
        for(int i = 24; i < 87; i++)
        {
            values.add(metIn.vectorOf(new boolean[]{false, true, false, true, false, false}));
        }
        //(1,3)
        for(int i = 87; i < 118; i++)
        {
            values.add(metIn.vectorOf(new boolean[]{false, false, true, true, false, false}));
        }
        //(2,1)
        for(int i = 118; i < 181; i++)
        {
            values.add(metIn.vectorOf(new boolean[]{true, false, false, false, true, false}));
        }
        //(2,2)
        for(int i = 181; i < 284; i++)
        {
            values.add(metIn.vectorOf(new boolean[]{false, true, false, false, true, false}));
        }
        //(2,3)
        for(int i = 284; i < 426; i++)
        {
            values.add(metIn.vectorOf(new boolean[]{false, false, true, false, true, false}));
        }
        //(3,1)
        for(int i = 426; i < 481; i++)
        {
            values.add(metIn.vectorOf(new boolean[]{true, false, false, false, false, true}));
        }
        //(3,2)
        for(int i = 481; i < 591; i++)
        {
            values.add(metIn.vectorOf(new boolean[]{false, true, false, false, false, true}));
        }
        //(3,3)
        for(int i = 591; i < 788; i++)
        {
            values.add(metIn.vectorOf(new boolean[]{false, false, true, false, false, true}));
        }
        final MetadataFactory metFac = metIn.getMetadataFactory();
        final Metadata meta = metFac.getMetadata(values.size(),
                                                 metFac.getDimension(),
                                                 metFac.getDatasetIdentifier("DRRC2", "SQIN", "HEFS"));
        return metIn.ofMulticategoryPairs(values, meta); //Construct the pairs
    }

    /**
     * Returns a set of discrete probability pairs without a baseline.
     * 
     * @return discrete probability pairs
     */

    public static DiscreteProbabilityPairs getDiscreteProbabilityPairsOne()
    {
        //Construct some probabilistic pairs, and use the same pairs as a reference for skill (i.e. skill = 0.0)
        final DataFactory metIn = DefaultDataFactory.getInstance();
        final List<PairOfDoubles> values = new ArrayList<>();
        values.add(metIn.pairOf(0, 3.0 / 5.0));
        values.add(metIn.pairOf(0, 1.0 / 5.0));
        values.add(metIn.pairOf(1, 2.0 / 5.0));
        values.add(metIn.pairOf(1, 3.0 / 5.0));
        values.add(metIn.pairOf(0, 0.0 / 5.0));
        values.add(metIn.pairOf(1, 1.0 / 5.0));
        final MetadataFactory metFac = metIn.getMetadataFactory();
        final Metadata meta = metFac.getMetadata(values.size(),
                                                 metFac.getDimension(),
                                                 metFac.getDatasetIdentifier("DRRC2", "SQIN", "HEFS"));
        return metIn.ofDiscreteProbabilityPairs(values, meta);
    }

    /**
     * Returns a set of discrete probability pairs with a baseline.
     * 
     * @return discrete probability pairs
     */

    public static DiscreteProbabilityPairs getDiscreteProbabilityPairsTwo()
    {
        //Construct some probabilistic pairs, and use some different pairs as a reference
        final DataFactory metIn = DefaultDataFactory.getInstance();
        final List<PairOfDoubles> values = new ArrayList<>();
        values.add(metIn.pairOf(0, 3.0 / 5.0));
        values.add(metIn.pairOf(0, 1.0 / 5.0));
        values.add(metIn.pairOf(1, 2.0 / 5.0));
        values.add(metIn.pairOf(1, 3.0 / 5.0));
        values.add(metIn.pairOf(0, 0.0 / 5.0));
        values.add(metIn.pairOf(1, 1.0 / 5.0));
        final List<PairOfDoubles> baseline = new ArrayList<>();
        baseline.add(metIn.pairOf(0, 2.0 / 5.0));
        baseline.add(metIn.pairOf(0, 2.0 / 5.0));
        baseline.add(metIn.pairOf(1, 5.0 / 5.0));
        baseline.add(metIn.pairOf(1, 3.0 / 5.0));
        baseline.add(metIn.pairOf(0, 4.0 / 5.0));
        baseline.add(metIn.pairOf(1, 1.0 / 5.0));
        final MetadataFactory metFac = metIn.getMetadataFactory();
        final Metadata main = metFac.getMetadata(values.size(),
                                                 metFac.getDimension(),
                                                 metFac.getDatasetIdentifier("DRRC2", "SQIN", "HEFS"));
        final Metadata base = metFac.getMetadata(values.size(),
                                                 metFac.getDimension(),
                                                 metFac.getDatasetIdentifier("DRRC2", "SQIN", "ESP"));
        return metIn.ofDiscreteProbabilityPairs(values, baseline, main, base);
    }

}
