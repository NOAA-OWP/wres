package wres.engine.statistics.metric;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.DichotomousPairs;
import wres.datamodel.DiscreteProbabilityPairs;
import wres.datamodel.EnsemblePairs;
import wres.datamodel.Metadata;
import wres.datamodel.MetadataFactory;
import wres.datamodel.MulticategoryPairs;
import wres.datamodel.PairOfDoubleAndVectorOfDoubles;
import wres.datamodel.PairOfDoubles;
import wres.datamodel.SingleValuedPairs;
import wres.datamodel.VectorOfBooleans;

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
        return metIn.ofSingleValuedPairs(values, metFac.getMetadata());
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
        final Metadata main = metFac.getMetadata(metFac.getDimension("CMS"),
                                                 metFac.getDatasetIdentifier("DRRC2", "SQIN", "HEFS"));
        final Metadata base = metFac.getMetadata(metFac.getDimension("CMS"),
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
        final Metadata meta = metFac.getMetadata(metFac.getDimension("CMS"),
                                                 metFac.getDatasetIdentifier("DRRC2", "SQIN", "HEFS"));
        return metIn.ofSingleValuedPairs(values, meta);
    }

    /**
     * Returns a moderately-sized test dataset of single-valued pairs without a baseline. The data are partitioned by
     * observed values of {1,2,3,4,5} with 100-pair chunks and corresponding predicted values of {6,7,8,9,10}. The data
     * are returned with a nominal lead time of 1.
     * 
     * @return single-valued pairs
     */

    public static SingleValuedPairs getSingleValuedPairsFour()
    {
        //Construct some single-valued pairs
        final List<PairOfDoubles> values = new ArrayList<>();
        final DataFactory metIn = DefaultDataFactory.getInstance();
        for(int i = 0; i < 5; i++)
        {
            for(int j = 0; j < 100; j++)
            {
                values.add(metIn.pairOf(i + 1, i + 6));
            }
        }
        final MetadataFactory metFac = metIn.getMetadataFactory();
        final Metadata meta = metFac.getMetadata(metFac.getDimension("CMS"),
                                                 metFac.getDatasetIdentifier("DRRC2", "SQIN", "HEFS"),
                                                 1);
        return metIn.ofSingleValuedPairs(values, meta);
    }

    /**
     * Returns a small test dataset with predictions and corresponding observations from location "103.1" from
     * https://github.com/NVE/RunoffTestData. The data are stored in
     * testinput/metricTestDataFactory/getSingleValuedPairsFive.asc
     * 
     * @return single-valued pairs
     * @throws IOException if the read fails
     */

    public static SingleValuedPairs getSingleValuedPairsFive() throws IOException
    {
        //Construct some pairs
        final List<PairOfDoubles> values = new ArrayList<>();
        final DataFactory metIn = DefaultDataFactory.getInstance();
        File file = new File("testinput/metricTestDataFactory/getSingleValuedPairsFive.asc");
        try (BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(file), "UTF-8")))
        {
            String line = null;
            while(Objects.nonNull(line = in.readLine()) && !line.isEmpty())
            {
                double[] doubleValues = Arrays.stream(line.split("\\s+")).mapToDouble(Double::parseDouble).toArray();
                values.add(metIn.pairOf(doubleValues[0], doubleValues[1]));
            }
        }
        final MetadataFactory metFac = metIn.getMetadataFactory();
        final Metadata meta = metFac.getMetadata(metFac.getDimension("MM/DAY"),
                                                 metFac.getDatasetIdentifier("103.1", "QME", "NVE"),
                                                 24);
        return metIn.ofSingleValuedPairs(values, meta);
    }

    /**
     * Returns a set of single-valued pairs with a single pair and no baseline. This is useful for checking exceptional
     * behaviour due to an inadequate sample size.
     * 
     * @return single-valued pairs
     */

    public static SingleValuedPairs getSingleValuedPairsSix()
    {
        //Construct some single-valued pairs
        final DataFactory metIn = DefaultDataFactory.getInstance();
        final List<PairOfDoubles> values = new ArrayList<>();
        values.add(metIn.pairOf(22.9, 22.8));
        final MetadataFactory metFac = metIn.getMetadataFactory();
        final Metadata meta = metFac.getMetadata(metFac.getDimension("MM/DAY"),
                                                 metFac.getDatasetIdentifier("A", "MAP"),
                                                 24);
        return metIn.ofSingleValuedPairs(values, meta);
    }    
    
    /**
     * Returns a moderately-sized test dataset of ensemble pairs without a baseline. Reads the pairs from
     * testinput/metricTestDataFactory/getEnsemblePairsOne.asc. The inputs have a lead time of 24 hours.
     * 
     * @return ensemble pairs
     * @throws IOException if the read fails
     */

    public static EnsemblePairs getEnsemblePairsOne() throws IOException
    {
        //Construct some ensemble pairs
        final List<PairOfDoubleAndVectorOfDoubles> values = new ArrayList<>();
        final DataFactory metIn = DefaultDataFactory.getInstance();
        File file = new File("testinput/metricTestDataFactory/getEnsemblePairsOne.asc");
        List<Double> climatology = new ArrayList<>();
        try (BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(file), "UTF-8")))
        {
            String line = null;
            while(Objects.nonNull(line = in.readLine()) && !line.isEmpty())
            {
                double[] doubleValues = Arrays.stream(line.split("\\s+")).mapToDouble(Double::parseDouble).toArray();
                values.add(metIn.pairOf(doubleValues[0], Arrays.copyOfRange(doubleValues, 1, doubleValues.length)));
                climatology.add(doubleValues[0]);
            }
        }
        final MetadataFactory metFac = metIn.getMetadataFactory();
        final Metadata meta = metFac.getMetadata(metFac.getDimension("CMS"),
                                                 metFac.getDatasetIdentifier("DRRC2", "SQIN", "HEFS"),
                                                 24);
        return metIn.ofEnsemblePairs(values, meta, metIn.vectorOf(climatology.toArray(new Double[climatology.size()])));
    }
    
    /**
     * Returns a set of ensemble pairs with a single pair and no baseline. This is useful for checking exceptional
     * behaviour due to an inadequate sample size.
     * 
     * @return ensemble pairs
     */

    public static EnsemblePairs getEnsemblePairsTwo()
    {
        //Construct some ensemble pairs
        final DataFactory metIn = DefaultDataFactory.getInstance();
        final List<PairOfDoubleAndVectorOfDoubles> values = new ArrayList<>();
        values.add(metIn.pairOf(22.9, new double[]{22.8,23.9}));
        final MetadataFactory metFac = metIn.getMetadataFactory();
        final Metadata meta = metFac.getMetadata(metFac.getDimension("MM/DAY"),
                                                 metFac.getDatasetIdentifier("A", "MAP"),
                                                 24);
        return metIn.ofEnsemblePairs(values, meta);
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
        final Metadata meta = metFac.getMetadata(metFac.getDimension(),
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
        final Metadata meta = metFac.getMetadata(metFac.getDimension(),
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
        final Metadata meta = metFac.getMetadata(metFac.getDimension(),
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
        final Metadata main = metFac.getMetadata(metFac.getDimension(),
                                                 metFac.getDatasetIdentifier("DRRC2", "SQIN", "HEFS"));
        final Metadata base = metFac.getMetadata(metFac.getDimension(),
                                                 metFac.getDatasetIdentifier("DRRC2", "SQIN", "ESP"));
        return metIn.ofDiscreteProbabilityPairs(values, baseline, main, base);
    }

    /**
     * <p>
     * Returns a set of discrete probability pairs that comprises probability of precipitation forecasts from the
     * Finnish Meteorological Institute for a 24h lead time, and corresponding observations, available here:
     * </p>
     * <p>
     * http://www.cawcr.gov.au/projects/verification/POP3/POP_3cat_2003.txt
     * </p>
     * 
     * @return a set of discrete probability pairs
     */

    public static DiscreteProbabilityPairs getDiscreteProbabilityPairsThree()
    {
        //Construct some probabilistic pairs, and use some different pairs as a reference
        final DataFactory metIn = DefaultDataFactory.getInstance();
        final List<PairOfDoubles> values = new ArrayList<>();
        values.add(metIn.pairOf(0, 0.3));
        values.add(metIn.pairOf(0, 0.1));
        values.add(metIn.pairOf(0, 0.1));
        values.add(metIn.pairOf(0, 0.2));
        values.add(metIn.pairOf(0, 0.2));
        values.add(metIn.pairOf(0, 0.1));
        values.add(metIn.pairOf(1, 0.4));
        values.add(metIn.pairOf(1, 0.7));
        values.add(metIn.pairOf(0, 0.7));
        values.add(metIn.pairOf(0, 0.2));
        values.add(metIn.pairOf(1, 0.2));
        values.add(metIn.pairOf(1, 1));
        values.add(metIn.pairOf(1, 0.7));
        values.add(metIn.pairOf(1, 0.7));
        values.add(metIn.pairOf(0, 0.6));
        values.add(metIn.pairOf(1, 0.4));
        values.add(metIn.pairOf(0, 0.3));
        values.add(metIn.pairOf(1, 0.7));
        values.add(metIn.pairOf(0, 0.9));
        values.add(metIn.pairOf(1, 0.8));
        values.add(metIn.pairOf(0, 0.2));
        values.add(metIn.pairOf(0, 0.2));
        values.add(metIn.pairOf(1, 0.8));
        values.add(metIn.pairOf(1, 0.5));
        values.add(metIn.pairOf(0, 0.3));
        values.add(metIn.pairOf(0, 0.1));
        values.add(metIn.pairOf(0, 0.2));
        values.add(metIn.pairOf(0, 0));
        values.add(metIn.pairOf(0, 0));
        values.add(metIn.pairOf(0, 0.8));
        values.add(metIn.pairOf(0, 0.5));
        values.add(metIn.pairOf(1, 0.7));
        values.add(metIn.pairOf(0, 0.8));
        values.add(metIn.pairOf(0, 0.2));
        values.add(metIn.pairOf(0, 0.2));
        values.add(metIn.pairOf(0, 0.3));
        values.add(metIn.pairOf(0, 0.1));
        values.add(metIn.pairOf(0, 0.1));
        values.add(metIn.pairOf(0, 0.1));
        values.add(metIn.pairOf(0, 0.2));
        values.add(metIn.pairOf(0, 0.2));
        values.add(metIn.pairOf(0, 0.2));
        values.add(metIn.pairOf(0, 0));
        values.add(metIn.pairOf(0, 0));
        values.add(metIn.pairOf(0, 0));
        values.add(metIn.pairOf(0, 0));
        values.add(metIn.pairOf(0, 0.2));
        values.add(metIn.pairOf(0, 0.2));
        values.add(metIn.pairOf(0, 0));
        values.add(metIn.pairOf(0, 0));
        values.add(metIn.pairOf(0, 0.1));
        values.add(metIn.pairOf(0, 0));
        values.add(metIn.pairOf(0, 0.1));
        values.add(metIn.pairOf(0, 0.1));
        values.add(metIn.pairOf(0, 0));
        values.add(metIn.pairOf(0, 0));
        values.add(metIn.pairOf(0, 0.1));
        values.add(metIn.pairOf(0, 0.1));
        values.add(metIn.pairOf(0, 0));
        values.add(metIn.pairOf(0, 0));
        values.add(metIn.pairOf(0, 0));
        values.add(metIn.pairOf(0, 0.5));
        values.add(metIn.pairOf(0, 0.2));
        values.add(metIn.pairOf(0, 0.5));
        values.add(metIn.pairOf(0, 0.3));
        values.add(metIn.pairOf(0, 0.3));
        values.add(metIn.pairOf(0, 0.2));
        values.add(metIn.pairOf(0, 0.2));
        values.add(metIn.pairOf(0, 0));
        values.add(metIn.pairOf(0, 0.1));
        values.add(metIn.pairOf(0, 0.2));
        values.add(metIn.pairOf(0, 0));
        values.add(metIn.pairOf(0, 0));
        values.add(metIn.pairOf(0, 0.2));
        values.add(metIn.pairOf(0, 0));
        values.add(metIn.pairOf(0, 0.3));
        values.add(metIn.pairOf(0, 0.2));
        values.add(metIn.pairOf(0, 0.2));
        values.add(metIn.pairOf(0, 0));
        values.add(metIn.pairOf(0, 0));
        values.add(metIn.pairOf(0, 0));
        values.add(metIn.pairOf(0, 0.1));
        values.add(metIn.pairOf(0, 0));
        values.add(metIn.pairOf(1, 0));
        values.add(metIn.pairOf(0, 0.3));
        values.add(metIn.pairOf(0, 0));
        values.add(metIn.pairOf(0, 0.2));
        values.add(metIn.pairOf(0, 0.2));
        values.add(metIn.pairOf(1, 0.9));
        values.add(metIn.pairOf(1, 0.6));
        values.add(metIn.pairOf(0, 0));
        values.add(metIn.pairOf(0, 0));
        values.add(metIn.pairOf(0, 0.3));
        values.add(metIn.pairOf(0, 0.3));
        values.add(metIn.pairOf(1, 0.8));
        values.add(metIn.pairOf(0, 0.5));
        values.add(metIn.pairOf(0, 0.7));
        values.add(metIn.pairOf(0, 0.6));
        values.add(metIn.pairOf(0, 0.3));
        values.add(metIn.pairOf(0, 0.3));
        values.add(metIn.pairOf(0, 0.1));
        values.add(metIn.pairOf(0, 0.1));
        values.add(metIn.pairOf(0, 0.1));
        values.add(metIn.pairOf(0, 0.1));
        values.add(metIn.pairOf(0, 0.2));
        values.add(metIn.pairOf(0, 0.2));
        values.add(metIn.pairOf(0, 0.1));
        values.add(metIn.pairOf(0, 0.2));
        values.add(metIn.pairOf(0, 0.2));
        values.add(metIn.pairOf(0, 0.2));
        values.add(metIn.pairOf(0, 0.2));
        values.add(metIn.pairOf(0, 0.2));
        values.add(metIn.pairOf(0, 0.3));
        values.add(metIn.pairOf(0, 0.7));
        values.add(metIn.pairOf(1, 1));
        values.add(metIn.pairOf(0, 0.2));
        values.add(metIn.pairOf(0, 0.1));
        values.add(metIn.pairOf(0, 0.2));
        values.add(metIn.pairOf(1, 0.9));
        values.add(metIn.pairOf(0, 0.6));
        values.add(metIn.pairOf(0, 0.1));
        values.add(metIn.pairOf(0, 0.1));
        values.add(metIn.pairOf(1, 0.2));
        values.add(metIn.pairOf(0, 0.1));
        values.add(metIn.pairOf(0, 0.8));
        values.add(metIn.pairOf(1, 1));
        values.add(metIn.pairOf(0, 0.1));
        values.add(metIn.pairOf(1, 1));
        values.add(metIn.pairOf(1, 0.8));
        values.add(metIn.pairOf(0, 0.5));
        values.add(metIn.pairOf(1, 0.6));
        values.add(metIn.pairOf(1, 0.5));
        values.add(metIn.pairOf(0, 0.2));
        values.add(metIn.pairOf(0, 0.1));
        values.add(metIn.pairOf(0, 0.2));
        values.add(metIn.pairOf(0, 0.7));
        values.add(metIn.pairOf(0, 0.2));
        values.add(metIn.pairOf(1, 0.8));
        values.add(metIn.pairOf(0, 0.1));
        values.add(metIn.pairOf(0, 0.2));
        values.add(metIn.pairOf(0, 0));
        values.add(metIn.pairOf(0, 0.6));
        values.add(metIn.pairOf(0, 0.1));
        values.add(metIn.pairOf(0, 0.1));
        values.add(metIn.pairOf(0, 0.2));
        values.add(metIn.pairOf(1, 0.3));
        values.add(metIn.pairOf(1, 0.3));
        values.add(metIn.pairOf(0, 0.7));
        values.add(metIn.pairOf(0, 0.6));
        values.add(metIn.pairOf(0, 0.1));
        values.add(metIn.pairOf(1, 0.9));
        values.add(metIn.pairOf(0, 0.2));
        values.add(metIn.pairOf(0, 0));
        values.add(metIn.pairOf(0, 0.7));
        values.add(metIn.pairOf(0, 0.7));
        values.add(metIn.pairOf(1, 0.5));
        values.add(metIn.pairOf(0, 0.7));
        values.add(metIn.pairOf(1, 0.6));
        values.add(metIn.pairOf(1, 0.4));
        values.add(metIn.pairOf(0, 0.2));
        values.add(metIn.pairOf(0, 0.8));
        values.add(metIn.pairOf(1, 0.7));
        values.add(metIn.pairOf(0, 0.4));
        values.add(metIn.pairOf(0, 0.3));
        values.add(metIn.pairOf(0, 0.2));
        values.add(metIn.pairOf(1, 0.3));
        values.add(metIn.pairOf(0, 0.3));
        values.add(metIn.pairOf(0, 0.7));
        values.add(metIn.pairOf(0, 0.2));
        values.add(metIn.pairOf(1, 0.7));
        values.add(metIn.pairOf(0, 0.8));
        values.add(metIn.pairOf(0, 0.4));
        values.add(metIn.pairOf(0, 0.2));
        values.add(metIn.pairOf(0, 0.4));
        values.add(metIn.pairOf(1, 0.7));
        values.add(metIn.pairOf(0, 0.3));
        values.add(metIn.pairOf(0, 0.5));
        values.add(metIn.pairOf(0, 0.5));
        values.add(metIn.pairOf(0, 0.1));
        values.add(metIn.pairOf(0, 0.1));
        values.add(metIn.pairOf(0, 0.7));
        values.add(metIn.pairOf(0, 0.3));
        values.add(metIn.pairOf(1, 0.2));
        values.add(metIn.pairOf(0, 0.3));
        values.add(metIn.pairOf(0, 0.2));
        values.add(metIn.pairOf(0, 0.2));
        values.add(metIn.pairOf(0, 0.3));
        values.add(metIn.pairOf(0, 0.2));
        values.add(metIn.pairOf(0, 0.5));
        values.add(metIn.pairOf(0, 0.5));
        values.add(metIn.pairOf(0, 0.2));
        values.add(metIn.pairOf(0, 0.3));
        values.add(metIn.pairOf(0, 0.4));
        values.add(metIn.pairOf(0, 0.4));
        values.add(metIn.pairOf(0, 0.4));
        values.add(metIn.pairOf(0, 0.6));
        values.add(metIn.pairOf(1, 0.8));
        values.add(metIn.pairOf(1, 0.8));
        values.add(metIn.pairOf(1, 0.9));
        values.add(metIn.pairOf(0, 0.6));
        values.add(metIn.pairOf(1, 0.5));
        values.add(metIn.pairOf(0, 0.4));
        values.add(metIn.pairOf(0, 0.4));
        values.add(metIn.pairOf(1, 0.6));
        values.add(metIn.pairOf(0, 0.7));
        values.add(metIn.pairOf(1, 0.8));
        values.add(metIn.pairOf(0, 0.3));
        values.add(metIn.pairOf(0, 0.1));
        values.add(metIn.pairOf(1, 0.5));
        values.add(metIn.pairOf(1, 0.3));
        values.add(metIn.pairOf(0, 0.7));
        values.add(metIn.pairOf(0, 0));
        values.add(metIn.pairOf(0, 0.2));
        values.add(metIn.pairOf(0, 0.3));
        values.add(metIn.pairOf(1, 0.7));
        values.add(metIn.pairOf(0, 0.6));
        values.add(metIn.pairOf(0, 0.4));
        values.add(metIn.pairOf(0, 0.2));
        values.add(metIn.pairOf(0, 0));
        values.add(metIn.pairOf(0, 0.2));
        values.add(metIn.pairOf(1, 0.7));
        values.add(metIn.pairOf(0, 0.7));
        values.add(metIn.pairOf(0, 0.5));
        values.add(metIn.pairOf(1, 0.9));
        values.add(metIn.pairOf(1, 0.7));
        values.add(metIn.pairOf(0, 0.4));
        values.add(metIn.pairOf(0, 0.6));
        values.add(metIn.pairOf(0, 0.7));
        values.add(metIn.pairOf(0, 1));
        values.add(metIn.pairOf(1, 0.8));
        values.add(metIn.pairOf(0, 0.3));
        values.add(metIn.pairOf(0, 0.6));
        values.add(metIn.pairOf(0, 0.5));
        values.add(metIn.pairOf(0, 0.1));
        values.add(metIn.pairOf(0, 0.5));
        values.add(metIn.pairOf(0, 0.3));
        values.add(metIn.pairOf(0, 0.1));
        values.add(metIn.pairOf(0, 0.1));
        values.add(metIn.pairOf(0, 0));
        values.add(metIn.pairOf(0, 0));
        values.add(metIn.pairOf(0, 0));
        values.add(metIn.pairOf(0, 0));
        values.add(metIn.pairOf(0, 0));
        values.add(metIn.pairOf(0, 0.2));
        values.add(metIn.pairOf(0, 0.4));
        values.add(metIn.pairOf(0, 0));
        values.add(metIn.pairOf(0, 0.2));
        values.add(metIn.pairOf(0, 0.1));
        values.add(metIn.pairOf(0, 0.3));
        values.add(metIn.pairOf(0, 0.3));
        values.add(metIn.pairOf(0, 0));
        values.add(metIn.pairOf(0, 0.6));
        values.add(metIn.pairOf(0, 0.3));
        values.add(metIn.pairOf(1, 0.9));
        values.add(metIn.pairOf(0, 0.1));
        values.add(metIn.pairOf(0, 0.7));
        values.add(metIn.pairOf(0, 1));
        values.add(metIn.pairOf(0, 0.9));
        values.add(metIn.pairOf(0, 0.2));
        values.add(metIn.pairOf(0, 0.5));
        values.add(metIn.pairOf(1, 0.8));
        values.add(metIn.pairOf(0, 0.3));
        values.add(metIn.pairOf(0, 0.6));
        values.add(metIn.pairOf(0, 0.6));
        values.add(metIn.pairOf(0, 0.7));
        values.add(metIn.pairOf(1, 0.8));
        values.add(metIn.pairOf(1, 1));
        values.add(metIn.pairOf(0, 0.3));
        values.add(metIn.pairOf(0, 0.2));
        values.add(metIn.pairOf(1, 1));
        values.add(metIn.pairOf(1, 0.7));
        values.add(metIn.pairOf(0, 0.4));
        values.add(metIn.pairOf(0, 0));
        values.add(metIn.pairOf(0, 0));
        values.add(metIn.pairOf(0, 0.1));
        values.add(metIn.pairOf(0, 0.1));
        values.add(metIn.pairOf(0, 0.3));
        values.add(metIn.pairOf(0, 0.3));
        values.add(metIn.pairOf(0, 0.2));
        values.add(metIn.pairOf(0, 0.1));
        values.add(metIn.pairOf(0, 0));
        values.add(metIn.pairOf(0, 0.1));
        values.add(metIn.pairOf(1, 0.8));
        values.add(metIn.pairOf(0, 0.8));
        values.add(metIn.pairOf(0, 0.3));
        values.add(metIn.pairOf(0, 0.1));
        values.add(metIn.pairOf(1, 0.5));
        values.add(metIn.pairOf(0, 0.4));
        values.add(metIn.pairOf(1, 0.7));
        values.add(metIn.pairOf(0, 0.3));
        values.add(metIn.pairOf(1, 0.8));
        values.add(metIn.pairOf(0, 0.7));
        values.add(metIn.pairOf(1, 0.7));
        values.add(metIn.pairOf(0, 0.1));
        values.add(metIn.pairOf(0, 0.1));
        values.add(metIn.pairOf(0, 0.1));
        values.add(metIn.pairOf(0, 0));
        values.add(metIn.pairOf(0, 0));
        values.add(metIn.pairOf(0, 0));
        values.add(metIn.pairOf(1, 0.5));
        values.add(metIn.pairOf(0, 0));
        values.add(metIn.pairOf(1, 1));
        values.add(metIn.pairOf(1, 0.7));
        values.add(metIn.pairOf(1, 0.5));
        values.add(metIn.pairOf(0, 0.6));
        values.add(metIn.pairOf(1, 0.4));
        values.add(metIn.pairOf(0, 0.3));
        values.add(metIn.pairOf(0, 0.4));
        values.add(metIn.pairOf(0, 0.3));
        values.add(metIn.pairOf(0, 0));
        values.add(metIn.pairOf(1, 0.8));
        values.add(metIn.pairOf(1, 1));
        values.add(metIn.pairOf(1, 1));
        values.add(metIn.pairOf(0, 0.8));
        values.add(metIn.pairOf(0, 0.5));
        values.add(metIn.pairOf(1, 0.2));
        values.add(metIn.pairOf(0, 0.1));
        values.add(metIn.pairOf(0, 0.1));
        values.add(metIn.pairOf(1, 0.2));
        values.add(metIn.pairOf(0, 0.1));
        values.add(metIn.pairOf(0, 0.1));
        values.add(metIn.pairOf(0, 0.9));
        values.add(metIn.pairOf(0, 0.1));
        values.add(metIn.pairOf(0, 0));
        values.add(metIn.pairOf(0, 0.6));
        values.add(metIn.pairOf(0, 0.7));
        values.add(metIn.pairOf(0, 0.1));
        values.add(metIn.pairOf(0, 0.1));
        values.add(metIn.pairOf(1, 1));
        values.add(metIn.pairOf(1, 0.6));
        values.add(metIn.pairOf(0, 0.2));
        values.add(metIn.pairOf(1, 0.8));
        values.add(metIn.pairOf(0, 0.8));
        values.add(metIn.pairOf(0, 0.6));
        values.add(metIn.pairOf(0, 0.4));
        values.add(metIn.pairOf(1, 0.6));
        values.add(metIn.pairOf(1, 0.3));
        values.add(metIn.pairOf(1, 0.1));
        values.add(metIn.pairOf(1, 0.9));
        values.add(metIn.pairOf(1, 0.7));
        values.add(metIn.pairOf(0, 0.3));
        values.add(metIn.pairOf(1, 0.8));
        values.add(metIn.pairOf(1, 1));
        values.add(metIn.pairOf(1, 0.9));
        values.add(metIn.pairOf(0, 0.1));
        values.add(metIn.pairOf(0, 0.1));
        values.add(metIn.pairOf(0, 0.1));
        final MetadataFactory metFac = metIn.getMetadataFactory();
        final Metadata main = metFac.getMetadata(metFac.getDimension(),
                                                 metFac.getDatasetIdentifier("Tampere", "MAP", "FMI"));
        return metIn.ofDiscreteProbabilityPairs(values, main);
    }

    /**
     * Returns a set of discrete probability pairs without a baseline and comprising observed non-occurrences only.
     * 
     * @return discrete probability pairs with observed non-occurrences
     */

    public static DiscreteProbabilityPairs getDiscreteProbabilityPairsFour()
    {
        final DataFactory metIn = DefaultDataFactory.getInstance();
        final List<PairOfDoubles> values = new ArrayList<>();
        values.add(metIn.pairOf(0, 3.0 / 5.0));
        values.add(metIn.pairOf(0, 1.0 / 5.0));
        values.add(metIn.pairOf(0, 2.0 / 5.0));
        values.add(metIn.pairOf(0, 3.0 / 5.0));
        values.add(metIn.pairOf(0, 0.0 / 5.0));
        values.add(metIn.pairOf(0, 1.0 / 5.0));
        final MetadataFactory metFac = metIn.getMetadataFactory();
        final Metadata meta = metFac.getMetadata(metFac.getDimension(),
                                                 metFac.getDatasetIdentifier("DRRC2", "SQIN", "HEFS"));
        return metIn.ofDiscreteProbabilityPairs(values, meta);
    }    
    
}
