package wres.engine.statistics.metric;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import wres.datamodel.DataFactory;
import wres.datamodel.Location;
import wres.datamodel.VectorOfBooleans;
import wres.datamodel.inputs.pairs.DichotomousPairs;
import wres.datamodel.inputs.pairs.DiscreteProbabilityPairs;
import wres.datamodel.inputs.pairs.EnsemblePairs;
import wres.datamodel.inputs.pairs.MulticategoryPairs;
import wres.datamodel.inputs.pairs.PairOfDoubleAndVectorOfDoubles;
import wres.datamodel.inputs.pairs.PairOfDoubles;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.inputs.pairs.TimeSeriesOfSingleValuedPairs;
import wres.datamodel.inputs.pairs.TimeSeriesOfSingleValuedPairsBuilder;
import wres.datamodel.metadata.Metadata;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.metadata.ReferenceTime;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.time.Event;

/**
 * Factory class for generating test datasets for metric calculations.
 * 
 * @author james.brown@hydrosolved.com
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
        final List<PairOfDoubles> values = new ArrayList<>();
        values.add( DataFactory.pairOf( 22.9, 22.8 ) );
        values.add( DataFactory.pairOf( 75.2, 80 ) );
        values.add( DataFactory.pairOf( 63.2, 65 ) );
        values.add( DataFactory.pairOf( 29, 30 ) );
        values.add( DataFactory.pairOf( 5, 2 ) );
        values.add( DataFactory.pairOf( 2.1, 3.1 ) );
        values.add( DataFactory.pairOf( 35000, 37000 ) );
        values.add( DataFactory.pairOf( 8, 7 ) );
        values.add( DataFactory.pairOf( 12, 12 ) );
        values.add( DataFactory.pairOf( 93, 94 ) );

        return DataFactory.ofSingleValuedPairs( values, MetadataFactory.getMetadata() );
    }

    /**
     * Returns a set of single-valued pairs with a baseline.
     * 
     * @return single-valued pairs
     */

    public static SingleValuedPairs getSingleValuedPairsTwo()
    {
        //Construct some single-valued pairs
        final List<PairOfDoubles> values = new ArrayList<>();
        values.add( DataFactory.pairOf( 22.9, 22.8 ) );
        values.add( DataFactory.pairOf( 75.2, 80 ) );
        values.add( DataFactory.pairOf( 63.2, 65 ) );
        values.add( DataFactory.pairOf( 29, 30 ) );
        values.add( DataFactory.pairOf( 5, 2 ) );
        values.add( DataFactory.pairOf( 2.1, 3.1 ) );
        values.add( DataFactory.pairOf( 35000, 37000 ) );
        values.add( DataFactory.pairOf( 8, 7 ) );
        values.add( DataFactory.pairOf( 12, 12 ) );
        values.add( DataFactory.pairOf( 93, 94 ) );
        final List<PairOfDoubles> baseline = new ArrayList<>();
        baseline.add( DataFactory.pairOf( 20.9, 23.8 ) );
        baseline.add( DataFactory.pairOf( 71.2, 83.2 ) );
        baseline.add( DataFactory.pairOf( 69.2, 66 ) );
        baseline.add( DataFactory.pairOf( 20, 30.5 ) );
        baseline.add( DataFactory.pairOf( 5.8, 2.1 ) );
        baseline.add( DataFactory.pairOf( 1.1, 3.4 ) );
        baseline.add( DataFactory.pairOf( 33020, 37500 ) );
        baseline.add( DataFactory.pairOf( 8.8, 7.1 ) );
        baseline.add( DataFactory.pairOf( 12.1, 13 ) );
        baseline.add( DataFactory.pairOf( 93.2, 94.8 ) );

        final Metadata main = MetadataFactory.getMetadata( MetadataFactory.getDimension( "CMS" ),
                                                           MetadataFactory.getDatasetIdentifier( getLocation( "DRRC2" ),
                                                                                                 "SQIN",
                                                                                                 "HEFS" ) );
        final Metadata base = MetadataFactory.getMetadata( MetadataFactory.getDimension( "CMS" ),
                                                           MetadataFactory.getDatasetIdentifier( getLocation( "DRRC2" ),
                                                                                                 "SQIN",
                                                                                                 "ESP" ) );
        return DataFactory.ofSingleValuedPairs( values, baseline, main, base );
    }

    public static Location getLocation( final String locationId )
    {
        return MetadataFactory.getLocation( locationId );
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

        for ( int i = 0; i < 10000; i++ )
        {
            values.add( DataFactory.pairOf( 5, 10 ) );
        }

        final Metadata meta = MetadataFactory.getMetadata( MetadataFactory.getDimension( "CMS" ),
                                                           MetadataFactory.getDatasetIdentifier( getLocation( "DRRC2" ),
                                                                                                 "SQIN",
                                                                                                 "HEFS" ) );
        return DataFactory.ofSingleValuedPairs( values, meta );
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

        for ( int i = 0; i < 5; i++ )
        {
            for ( int j = 0; j < 100; j++ )
            {
                values.add( DataFactory.pairOf( i + 1, i + 6 ) );
            }
        }

        final TimeWindow window = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                 Instant.parse( "2010-12-31T11:59:59Z" ),
                                                 ReferenceTime.VALID_TIME,
                                                 Duration.ofHours( 1 ) );
        final Metadata meta = MetadataFactory.getMetadata( MetadataFactory.getDimension( "CMS" ),
                                                           MetadataFactory.getDatasetIdentifier( getLocation( "DRRC2" ),
                                                                                                 "SQIN",
                                                                                                 "HEFS" ),
                                                           window );
        return DataFactory.ofSingleValuedPairs( values, meta );
    }

    /**
     * <p>Returns a small test dataset with predictions and corresponding observations from location "103.1" from
     * https://github.com/NVE/RunoffTestData:
     * 
     * https://github.com/NVE/RunoffTestData/blob/master/24h/qobs_calib/103.1.txt
     * https://github.com/NVE/RunoffTestData/blob/master/Example/calib_txt/103_1_station.txt
     * </p>
     * 
     * <p>The data are stored in:</p>
     *  
     * <p>testinput/metricTestDataFactory/getSingleValuedPairsFive.asc</p>
     * 
     * <p>The observations are in the first column and the predictions are in the second column.</p>
     * 
     * @return single-valued pairs
     * @throws IOException if the read fails
     */

    public static SingleValuedPairs getSingleValuedPairsFive() throws IOException
    {
        //Construct some pairs
        final List<PairOfDoubles> values = new ArrayList<>();

        File file = new File( "testinput/metricTestDataFactory/getSingleValuedPairsFive.asc" );
        try ( BufferedReader in = new BufferedReader( new InputStreamReader( new FileInputStream( file ), "UTF-8" ) ) )
        {
            String line = null;
            while ( Objects.nonNull( line = in.readLine() ) && !line.isEmpty() )
            {
                double[] doubleValues =
                        Arrays.stream( line.split( "\\s+" ) ).mapToDouble( Double::parseDouble ).toArray();
                values.add( DataFactory.pairOf( doubleValues[0], doubleValues[1] ) );
            }
        }

        final TimeWindow window = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                 Instant.parse( "2010-12-31T11:59:59Z" ),
                                                 ReferenceTime.VALID_TIME,
                                                 Duration.ofHours( 24 ) );
        final Metadata meta = MetadataFactory.getMetadata( MetadataFactory.getDimension( "MM/DAY" ),
                                                           MetadataFactory.getDatasetIdentifier( getLocation( "103.1" ),
                                                                                                 "QME",
                                                                                                 "NVE" ),
                                                           window );
        return DataFactory.ofSingleValuedPairs( values, meta );
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

        final List<PairOfDoubles> values = new ArrayList<>();
        values.add( DataFactory.pairOf( 22.9, 22.8 ) );
        final TimeWindow window = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                 Instant.parse( "2010-12-31T11:59:59Z" ),
                                                 ReferenceTime.VALID_TIME,
                                                 Duration.ofHours( 24 ) );
        final Metadata meta = MetadataFactory.getMetadata( MetadataFactory.getDimension( "MM/DAY" ),
                                                           MetadataFactory.getDatasetIdentifier( getLocation( "A" ),
                                                                                                 "MAP" ),
                                                           window );
        return DataFactory.ofSingleValuedPairs( values, meta );
    }

    /**
     * Returns a set of single-valued pairs with a baseline, both empty.
     * 
     * @return single-valued pairs
     */

    public static SingleValuedPairs getSingleValuedPairsSeven()
    {
        //Construct some single-valued pairs
        final Metadata main = MetadataFactory.getMetadata( MetadataFactory.getDimension( "CMS" ),
                                                           MetadataFactory.getDatasetIdentifier( getLocation( "DRRC2" ),
                                                                                                 "SQIN",
                                                                                                 "HEFS" ) );
        final Metadata base = MetadataFactory.getMetadata( MetadataFactory.getDimension( "CMS" ),
                                                           MetadataFactory.getDatasetIdentifier( getLocation( "DRRC2" ),
                                                                                                 "SQIN",
                                                                                                 "ESP" ) );
        return DataFactory.ofSingleValuedPairs( Collections.emptyList(), Collections.emptyList(), main, base );
    }

    /**
     * Returns a moderately-sized test dataset of ensemble pairs with the same dataset as a baseline. Reads the pairs 
     * from testinput/metricTestDataFactory/getEnsemblePairsOne.asc. The inputs have a lead time of 24 hours.
     * 
     * @return ensemble pairs
     * @throws IOException if the read fails
     */

    public static EnsemblePairs getEnsemblePairsOne() throws IOException
    {
        //Construct some ensemble pairs
        final List<PairOfDoubleAndVectorOfDoubles> values = new ArrayList<>();

        File file = new File( "testinput/metricTestDataFactory/getEnsemblePairsOne.asc" );
        List<Double> climatology = new ArrayList<>();
        try ( BufferedReader in = new BufferedReader( new InputStreamReader( new FileInputStream( file ), "UTF-8" ) ) )
        {
            String line = null;
            while ( Objects.nonNull( line = in.readLine() ) && !line.isEmpty() )
            {
                double[] doubleValues =
                        Arrays.stream( line.split( "\\s+" ) ).mapToDouble( Double::parseDouble ).toArray();
                values.add( DataFactory.pairOf( doubleValues[0],
                                                Arrays.copyOfRange( doubleValues, 1, doubleValues.length ) ) );
                climatology.add( doubleValues[0] );
            }
        }

        final TimeWindow window = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                 Instant.parse( "2010-12-31T11:59:59Z" ),
                                                 ReferenceTime.VALID_TIME,
                                                 Duration.ofHours( 24 ) );
        final Metadata meta = MetadataFactory.getMetadata( MetadataFactory.getDimension( "CMS" ),
                                                           MetadataFactory.getDatasetIdentifier( getLocation( "DRRC2" ),
                                                                                                 "SQIN",
                                                                                                 "HEFS" ),
                                                           window );
        final Metadata baseMeta = MetadataFactory.getMetadata( MetadataFactory.getDimension( "CMS" ),
                                                               MetadataFactory.getDatasetIdentifier( getLocation( "DRRC2" ),
                                                                                                     "SQIN",
                                                                                                     "ESP" ),
                                                               window );
        return DataFactory.ofEnsemblePairs( values,
                                            values,
                                            meta,
                                            baseMeta,
                                            DataFactory.vectorOf( climatology.toArray( new Double[climatology.size()] ) ) );
    }

    /**
     * Returns a moderately-sized test dataset of ensemble pairs without a baseline. Reads the pairs from
     * testinput/metricTestDataFactory/getEnsemblePairsOne.asc. The inputs have a lead time of 24 hours. Adds some
     * missing values to the dataset, namely {@link Double#NaN}.
     * 
     * @return ensemble pairs
     * @throws IOException if the read fails
     */

    public static EnsemblePairs getEnsemblePairsOneWithMissings() throws IOException
    {
        //Construct some ensemble pairs
        final List<PairOfDoubleAndVectorOfDoubles> values = new ArrayList<>();

        File file = new File( "testinput/metricTestDataFactory/getEnsemblePairsOne.asc" );
        List<Double> climatology = new ArrayList<>();
        try ( BufferedReader in = new BufferedReader( new InputStreamReader( new FileInputStream( file ), "UTF-8" ) ) )
        {
            String line = null;
            while ( Objects.nonNull( line = in.readLine() ) && !line.isEmpty() )
            {
                double[] doubleValues =
                        Arrays.stream( line.split( "\\s+" ) ).mapToDouble( Double::parseDouble ).toArray();
                values.add( DataFactory.pairOf( doubleValues[0],
                                                Arrays.copyOfRange( doubleValues, 1, doubleValues.length ) ) );
                climatology.add( doubleValues[0] );
            }
        }
        //Add some missing values
        climatology.add( Double.NaN );
        values.add( DataFactory.pairOf( Double.NaN,
                                        new double[] { Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN } ) );

        final TimeWindow window = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                 Instant.parse( "2010-12-31T11:59:59Z" ),
                                                 ReferenceTime.VALID_TIME,
                                                 Duration.ofHours( 24 ) );
        final Metadata meta = MetadataFactory.getMetadata( MetadataFactory.getDimension( "CMS" ),
                                                           MetadataFactory.getDatasetIdentifier( getLocation( "DRRC2" ),
                                                                                                 "SQIN",
                                                                                                 "HEFS" ),
                                                           window );

        final Metadata baseMeta = MetadataFactory.getMetadata( MetadataFactory.getDimension( "CMS" ),
                                                               MetadataFactory.getDatasetIdentifier( getLocation( "DRRC2" ),
                                                                                                     "SQIN",
                                                                                                     "ESP" ),
                                                               window );

        return DataFactory.ofEnsemblePairs( values,
                                            values,
                                            meta,
                                            baseMeta,
                                            DataFactory.vectorOf( climatology.toArray( new Double[climatology.size()] ) ) );
    }

    /**
     * Returns a small test dataset of ensemble pairs without a baseline. Reads the pairs from
     * testinput/metricTestDataFactory/getEnsemblePairsTwo.asc. The inputs have a lead time of 24 hours.
     * 
     * @return ensemble pairs
     * @throws IOException if the read fails
     */

    public static EnsemblePairs getEnsemblePairsTwo() throws IOException
    {
        //Construct some ensemble pairs
        final List<PairOfDoubleAndVectorOfDoubles> values = new ArrayList<>();

        File file = new File( "testinput/metricTestDataFactory/getEnsemblePairsTwo.asc" );
        List<Double> climatology = new ArrayList<>();
        try ( BufferedReader in = new BufferedReader( new InputStreamReader( new FileInputStream( file ), "UTF-8" ) ) )
        {
            String line = null;
            while ( Objects.nonNull( line = in.readLine() ) && !line.isEmpty() )
            {
                double[] doubleValues =
                        Arrays.stream( line.split( "\\s+" ) ).mapToDouble( Double::parseDouble ).toArray();
                values.add( DataFactory.pairOf( doubleValues[0],
                                                Arrays.copyOfRange( doubleValues, 1, doubleValues.length ) ) );
                climatology.add( doubleValues[0] );
            }
        }

        final TimeWindow window = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                 Instant.parse( "2010-12-31T11:59:59Z" ),
                                                 ReferenceTime.VALID_TIME,
                                                 Duration.ofHours( 24 ) );
        final Metadata meta = MetadataFactory.getMetadata( MetadataFactory.getDimension( "CMS" ),
                                                           MetadataFactory.getDatasetIdentifier( getLocation( "DRRC2" ),
                                                                                                 "SQIN",
                                                                                                 "HEFS" ),
                                                           window );
        return DataFactory.ofEnsemblePairs( values,
                                            meta,
                                            DataFactory.vectorOf( climatology.toArray( new Double[climatology.size()] ) ) );
    }

    /**
     * Returns a set of ensemble pairs with a single pair and no baseline. 
     * 
     * @return ensemble pairs
     */

    public static EnsemblePairs getEnsemblePairsThree()
    {
        //Construct some ensemble pairs

        final List<PairOfDoubleAndVectorOfDoubles> values = new ArrayList<>();
        values.add( DataFactory.pairOf( 22.9, new double[] { 22.8, 23.9 } ) );

        final TimeWindow window = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                 Instant.parse( "2010-12-31T11:59:59Z" ),
                                                 ReferenceTime.VALID_TIME,
                                                 Duration.ofHours( 24 ) );
        final Metadata meta = MetadataFactory.getMetadata( MetadataFactory.getDimension( "MM/DAY" ),
                                                           MetadataFactory.getDatasetIdentifier( getLocation( "DRRC2" ),
                                                                                                 "MAP" ),
                                                           window );
        return DataFactory.ofEnsemblePairs( values, meta );
    }

    /**
     * Returns a set of ensemble pairs with no data in the main input or baseline. 
     * 
     * @return ensemble pairs
     */

    public static EnsemblePairs getEnsemblePairsFour()
    {
        //Construct some ensemble pairs
        final TimeWindow window = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                 Instant.parse( "2010-12-31T11:59:59Z" ),
                                                 ReferenceTime.VALID_TIME,
                                                 Duration.ofHours( 24 ) );
        final Metadata meta = MetadataFactory.getMetadata( MetadataFactory.getDimension( "MM/DAY" ),
                                                           MetadataFactory.getDatasetIdentifier( getLocation( "DRRC2" ),
                                                                                                 "MAP" ),
                                                           window );
        return DataFactory.ofEnsemblePairs( Collections.emptyList(), Collections.emptyList(), meta, meta );
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
        final List<VectorOfBooleans> values = new ArrayList<>();
        //Hits
        for ( int i = 0; i < 82; i++ )
        {
            values.add( DataFactory.vectorOf( new boolean[] { true, true } ) );
        }
        //False alarms
        for ( int i = 82; i < 120; i++ )
        {
            values.add( DataFactory.vectorOf( new boolean[] { false, true } ) );
        }
        //Misses
        for ( int i = 120; i < 143; i++ )
        {
            values.add( DataFactory.vectorOf( new boolean[] { true, false } ) );
        }
        for ( int i = 144; i < 366; i++ )
        {
            values.add( DataFactory.vectorOf( new boolean[] { false, false } ) );
        }

        final Metadata meta = MetadataFactory.getMetadata( MetadataFactory.getDimension(),
                                                           MetadataFactory.getDatasetIdentifier( getLocation( "DRRC2" ),
                                                                                                 "SQIN",
                                                                                                 "HEFS" ) );
        return DataFactory.ofDichotomousPairs( values, meta ); //Construct the pairs
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
        final List<VectorOfBooleans> values = new ArrayList<>();
        //(1,1)
        for ( int i = 0; i < 24; i++ )
        {
            values.add( DataFactory.vectorOf( new boolean[] { true, false, false, true, false, false } ) );
        }
        //(1,2)
        for ( int i = 24; i < 87; i++ )
        {
            values.add( DataFactory.vectorOf( new boolean[] { false, true, false, true, false, false } ) );
        }
        //(1,3)
        for ( int i = 87; i < 118; i++ )
        {
            values.add( DataFactory.vectorOf( new boolean[] { false, false, true, true, false, false } ) );
        }
        //(2,1)
        for ( int i = 118; i < 181; i++ )
        {
            values.add( DataFactory.vectorOf( new boolean[] { true, false, false, false, true, false } ) );
        }
        //(2,2)
        for ( int i = 181; i < 284; i++ )
        {
            values.add( DataFactory.vectorOf( new boolean[] { false, true, false, false, true, false } ) );
        }
        //(2,3)
        for ( int i = 284; i < 426; i++ )
        {
            values.add( DataFactory.vectorOf( new boolean[] { false, false, true, false, true, false } ) );
        }
        //(3,1)
        for ( int i = 426; i < 481; i++ )
        {
            values.add( DataFactory.vectorOf( new boolean[] { true, false, false, false, false, true } ) );
        }
        //(3,2)
        for ( int i = 481; i < 591; i++ )
        {
            values.add( DataFactory.vectorOf( new boolean[] { false, true, false, false, false, true } ) );
        }
        //(3,3)
        for ( int i = 591; i < 788; i++ )
        {
            values.add( DataFactory.vectorOf( new boolean[] { false, false, true, false, false, true } ) );
        }

        final Metadata meta = MetadataFactory.getMetadata( MetadataFactory.getDimension(),
                                                           MetadataFactory.getDatasetIdentifier( getLocation( "DRRC2" ),
                                                                                                 "SQIN",
                                                                                                 "HEFS" ) );
        return DataFactory.ofMulticategoryPairs( values, meta ); //Construct the pairs
    }

    /**
     * Returns a set of discrete probability pairs without a baseline.
     * 
     * @return discrete probability pairs
     */

    public static DiscreteProbabilityPairs getDiscreteProbabilityPairsOne()
    {
        //Construct some probabilistic pairs, and use the same pairs as a reference for skill (i.e. skill = 0.0)
        final List<PairOfDoubles> values = new ArrayList<>();
        values.add( DataFactory.pairOf( 0, 3.0 / 5.0 ) );
        values.add( DataFactory.pairOf( 0, 1.0 / 5.0 ) );
        values.add( DataFactory.pairOf( 1, 2.0 / 5.0 ) );
        values.add( DataFactory.pairOf( 1, 3.0 / 5.0 ) );
        values.add( DataFactory.pairOf( 0, 0.0 / 5.0 ) );
        values.add( DataFactory.pairOf( 1, 1.0 / 5.0 ) );

        final Metadata meta = MetadataFactory.getMetadata( MetadataFactory.getDimension(),
                                                           MetadataFactory.getDatasetIdentifier( getLocation( "DRRC2" ),
                                                                                                 "SQIN",
                                                                                                 "HEFS" ) );
        return DataFactory.ofDiscreteProbabilityPairs( values, meta );
    }

    /**
     * Returns a set of discrete probability pairs with a baseline.
     * 
     * @return discrete probability pairs
     */

    public static DiscreteProbabilityPairs getDiscreteProbabilityPairsTwo()
    {
        //Construct some probabilistic pairs, and use some different pairs as a reference
        final List<PairOfDoubles> values = new ArrayList<>();
        values.add( DataFactory.pairOf( 0, 3.0 / 5.0 ) );
        values.add( DataFactory.pairOf( 0, 1.0 / 5.0 ) );
        values.add( DataFactory.pairOf( 1, 2.0 / 5.0 ) );
        values.add( DataFactory.pairOf( 1, 3.0 / 5.0 ) );
        values.add( DataFactory.pairOf( 0, 0.0 / 5.0 ) );
        values.add( DataFactory.pairOf( 1, 1.0 / 5.0 ) );
        final List<PairOfDoubles> baseline = new ArrayList<>();
        baseline.add( DataFactory.pairOf( 0, 2.0 / 5.0 ) );
        baseline.add( DataFactory.pairOf( 0, 2.0 / 5.0 ) );
        baseline.add( DataFactory.pairOf( 1, 5.0 / 5.0 ) );
        baseline.add( DataFactory.pairOf( 1, 3.0 / 5.0 ) );
        baseline.add( DataFactory.pairOf( 0, 4.0 / 5.0 ) );
        baseline.add( DataFactory.pairOf( 1, 1.0 / 5.0 ) );
        final Metadata main = MetadataFactory.getMetadata( MetadataFactory.getDimension(),
                                                           MetadataFactory.getDatasetIdentifier( getLocation( "DRRC2" ),
                                                                                                 "SQIN",
                                                                                                 "HEFS" ) );
        final Metadata base = MetadataFactory.getMetadata( MetadataFactory.getDimension(),
                                                           MetadataFactory.getDatasetIdentifier( getLocation( "DRRC2" ),
                                                                                                 "SQIN",
                                                                                                 "ESP" ) );
        return DataFactory.ofDiscreteProbabilityPairs( values, baseline, main, base );
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
        final List<PairOfDoubles> values = new ArrayList<>();
        values.add( DataFactory.pairOf( 0, 0.3 ) );
        values.add( DataFactory.pairOf( 0, 0.1 ) );
        values.add( DataFactory.pairOf( 0, 0.1 ) );
        values.add( DataFactory.pairOf( 0, 0.2 ) );
        values.add( DataFactory.pairOf( 0, 0.2 ) );
        values.add( DataFactory.pairOf( 0, 0.1 ) );
        values.add( DataFactory.pairOf( 1, 0.4 ) );
        values.add( DataFactory.pairOf( 1, 0.7 ) );
        values.add( DataFactory.pairOf( 0, 0.7 ) );
        values.add( DataFactory.pairOf( 0, 0.2 ) );
        values.add( DataFactory.pairOf( 1, 0.2 ) );
        values.add( DataFactory.pairOf( 1, 1 ) );
        values.add( DataFactory.pairOf( 1, 0.7 ) );
        values.add( DataFactory.pairOf( 1, 0.7 ) );
        values.add( DataFactory.pairOf( 0, 0.6 ) );
        values.add( DataFactory.pairOf( 1, 0.4 ) );
        values.add( DataFactory.pairOf( 0, 0.3 ) );
        values.add( DataFactory.pairOf( 1, 0.7 ) );
        values.add( DataFactory.pairOf( 0, 0.9 ) );
        values.add( DataFactory.pairOf( 1, 0.8 ) );
        values.add( DataFactory.pairOf( 0, 0.2 ) );
        values.add( DataFactory.pairOf( 0, 0.2 ) );
        values.add( DataFactory.pairOf( 1, 0.8 ) );
        values.add( DataFactory.pairOf( 1, 0.5 ) );
        values.add( DataFactory.pairOf( 0, 0.3 ) );
        values.add( DataFactory.pairOf( 0, 0.1 ) );
        values.add( DataFactory.pairOf( 0, 0.2 ) );
        values.add( DataFactory.pairOf( 0, 0 ) );
        values.add( DataFactory.pairOf( 0, 0 ) );
        values.add( DataFactory.pairOf( 0, 0.8 ) );
        values.add( DataFactory.pairOf( 0, 0.5 ) );
        values.add( DataFactory.pairOf( 1, 0.7 ) );
        values.add( DataFactory.pairOf( 0, 0.8 ) );
        values.add( DataFactory.pairOf( 0, 0.2 ) );
        values.add( DataFactory.pairOf( 0, 0.2 ) );
        values.add( DataFactory.pairOf( 0, 0.3 ) );
        values.add( DataFactory.pairOf( 0, 0.1 ) );
        values.add( DataFactory.pairOf( 0, 0.1 ) );
        values.add( DataFactory.pairOf( 0, 0.1 ) );
        values.add( DataFactory.pairOf( 0, 0.2 ) );
        values.add( DataFactory.pairOf( 0, 0.2 ) );
        values.add( DataFactory.pairOf( 0, 0.2 ) );
        values.add( DataFactory.pairOf( 0, 0 ) );
        values.add( DataFactory.pairOf( 0, 0 ) );
        values.add( DataFactory.pairOf( 0, 0 ) );
        values.add( DataFactory.pairOf( 0, 0 ) );
        values.add( DataFactory.pairOf( 0, 0.2 ) );
        values.add( DataFactory.pairOf( 0, 0.2 ) );
        values.add( DataFactory.pairOf( 0, 0 ) );
        values.add( DataFactory.pairOf( 0, 0 ) );
        values.add( DataFactory.pairOf( 0, 0.1 ) );
        values.add( DataFactory.pairOf( 0, 0 ) );
        values.add( DataFactory.pairOf( 0, 0.1 ) );
        values.add( DataFactory.pairOf( 0, 0.1 ) );
        values.add( DataFactory.pairOf( 0, 0 ) );
        values.add( DataFactory.pairOf( 0, 0 ) );
        values.add( DataFactory.pairOf( 0, 0.1 ) );
        values.add( DataFactory.pairOf( 0, 0.1 ) );
        values.add( DataFactory.pairOf( 0, 0 ) );
        values.add( DataFactory.pairOf( 0, 0 ) );
        values.add( DataFactory.pairOf( 0, 0 ) );
        values.add( DataFactory.pairOf( 0, 0.5 ) );
        values.add( DataFactory.pairOf( 0, 0.2 ) );
        values.add( DataFactory.pairOf( 0, 0.5 ) );
        values.add( DataFactory.pairOf( 0, 0.3 ) );
        values.add( DataFactory.pairOf( 0, 0.3 ) );
        values.add( DataFactory.pairOf( 0, 0.2 ) );
        values.add( DataFactory.pairOf( 0, 0.2 ) );
        values.add( DataFactory.pairOf( 0, 0 ) );
        values.add( DataFactory.pairOf( 0, 0.1 ) );
        values.add( DataFactory.pairOf( 0, 0.2 ) );
        values.add( DataFactory.pairOf( 0, 0 ) );
        values.add( DataFactory.pairOf( 0, 0 ) );
        values.add( DataFactory.pairOf( 0, 0.2 ) );
        values.add( DataFactory.pairOf( 0, 0 ) );
        values.add( DataFactory.pairOf( 0, 0.3 ) );
        values.add( DataFactory.pairOf( 0, 0.2 ) );
        values.add( DataFactory.pairOf( 0, 0.2 ) );
        values.add( DataFactory.pairOf( 0, 0 ) );
        values.add( DataFactory.pairOf( 0, 0 ) );
        values.add( DataFactory.pairOf( 0, 0 ) );
        values.add( DataFactory.pairOf( 0, 0.1 ) );
        values.add( DataFactory.pairOf( 0, 0 ) );
        values.add( DataFactory.pairOf( 1, 0 ) );
        values.add( DataFactory.pairOf( 0, 0.3 ) );
        values.add( DataFactory.pairOf( 0, 0 ) );
        values.add( DataFactory.pairOf( 0, 0.2 ) );
        values.add( DataFactory.pairOf( 0, 0.2 ) );
        values.add( DataFactory.pairOf( 1, 0.9 ) );
        values.add( DataFactory.pairOf( 1, 0.6 ) );
        values.add( DataFactory.pairOf( 0, 0 ) );
        values.add( DataFactory.pairOf( 0, 0 ) );
        values.add( DataFactory.pairOf( 0, 0.3 ) );
        values.add( DataFactory.pairOf( 0, 0.3 ) );
        values.add( DataFactory.pairOf( 1, 0.8 ) );
        values.add( DataFactory.pairOf( 0, 0.5 ) );
        values.add( DataFactory.pairOf( 0, 0.7 ) );
        values.add( DataFactory.pairOf( 0, 0.6 ) );
        values.add( DataFactory.pairOf( 0, 0.3 ) );
        values.add( DataFactory.pairOf( 0, 0.3 ) );
        values.add( DataFactory.pairOf( 0, 0.1 ) );
        values.add( DataFactory.pairOf( 0, 0.1 ) );
        values.add( DataFactory.pairOf( 0, 0.1 ) );
        values.add( DataFactory.pairOf( 0, 0.1 ) );
        values.add( DataFactory.pairOf( 0, 0.2 ) );
        values.add( DataFactory.pairOf( 0, 0.2 ) );
        values.add( DataFactory.pairOf( 0, 0.1 ) );
        values.add( DataFactory.pairOf( 0, 0.2 ) );
        values.add( DataFactory.pairOf( 0, 0.2 ) );
        values.add( DataFactory.pairOf( 0, 0.2 ) );
        values.add( DataFactory.pairOf( 0, 0.2 ) );
        values.add( DataFactory.pairOf( 0, 0.2 ) );
        values.add( DataFactory.pairOf( 0, 0.3 ) );
        values.add( DataFactory.pairOf( 0, 0.7 ) );
        values.add( DataFactory.pairOf( 1, 1 ) );
        values.add( DataFactory.pairOf( 0, 0.2 ) );
        values.add( DataFactory.pairOf( 0, 0.1 ) );
        values.add( DataFactory.pairOf( 0, 0.2 ) );
        values.add( DataFactory.pairOf( 1, 0.9 ) );
        values.add( DataFactory.pairOf( 0, 0.6 ) );
        values.add( DataFactory.pairOf( 0, 0.1 ) );
        values.add( DataFactory.pairOf( 0, 0.1 ) );
        values.add( DataFactory.pairOf( 1, 0.2 ) );
        values.add( DataFactory.pairOf( 0, 0.1 ) );
        values.add( DataFactory.pairOf( 0, 0.8 ) );
        values.add( DataFactory.pairOf( 1, 1 ) );
        values.add( DataFactory.pairOf( 0, 0.1 ) );
        values.add( DataFactory.pairOf( 1, 1 ) );
        values.add( DataFactory.pairOf( 1, 0.8 ) );
        values.add( DataFactory.pairOf( 0, 0.5 ) );
        values.add( DataFactory.pairOf( 1, 0.6 ) );
        values.add( DataFactory.pairOf( 1, 0.5 ) );
        values.add( DataFactory.pairOf( 0, 0.2 ) );
        values.add( DataFactory.pairOf( 0, 0.1 ) );
        values.add( DataFactory.pairOf( 0, 0.2 ) );
        values.add( DataFactory.pairOf( 0, 0.7 ) );
        values.add( DataFactory.pairOf( 0, 0.2 ) );
        values.add( DataFactory.pairOf( 1, 0.8 ) );
        values.add( DataFactory.pairOf( 0, 0.1 ) );
        values.add( DataFactory.pairOf( 0, 0.2 ) );
        values.add( DataFactory.pairOf( 0, 0 ) );
        values.add( DataFactory.pairOf( 0, 0.6 ) );
        values.add( DataFactory.pairOf( 0, 0.1 ) );
        values.add( DataFactory.pairOf( 0, 0.1 ) );
        values.add( DataFactory.pairOf( 0, 0.2 ) );
        values.add( DataFactory.pairOf( 1, 0.3 ) );
        values.add( DataFactory.pairOf( 1, 0.3 ) );
        values.add( DataFactory.pairOf( 0, 0.7 ) );
        values.add( DataFactory.pairOf( 0, 0.6 ) );
        values.add( DataFactory.pairOf( 0, 0.1 ) );
        values.add( DataFactory.pairOf( 1, 0.9 ) );
        values.add( DataFactory.pairOf( 0, 0.2 ) );
        values.add( DataFactory.pairOf( 0, 0 ) );
        values.add( DataFactory.pairOf( 0, 0.7 ) );
        values.add( DataFactory.pairOf( 0, 0.7 ) );
        values.add( DataFactory.pairOf( 1, 0.5 ) );
        values.add( DataFactory.pairOf( 0, 0.7 ) );
        values.add( DataFactory.pairOf( 1, 0.6 ) );
        values.add( DataFactory.pairOf( 1, 0.4 ) );
        values.add( DataFactory.pairOf( 0, 0.2 ) );
        values.add( DataFactory.pairOf( 0, 0.8 ) );
        values.add( DataFactory.pairOf( 1, 0.7 ) );
        values.add( DataFactory.pairOf( 0, 0.4 ) );
        values.add( DataFactory.pairOf( 0, 0.3 ) );
        values.add( DataFactory.pairOf( 0, 0.2 ) );
        values.add( DataFactory.pairOf( 1, 0.3 ) );
        values.add( DataFactory.pairOf( 0, 0.3 ) );
        values.add( DataFactory.pairOf( 0, 0.7 ) );
        values.add( DataFactory.pairOf( 0, 0.2 ) );
        values.add( DataFactory.pairOf( 1, 0.7 ) );
        values.add( DataFactory.pairOf( 0, 0.8 ) );
        values.add( DataFactory.pairOf( 0, 0.4 ) );
        values.add( DataFactory.pairOf( 0, 0.2 ) );
        values.add( DataFactory.pairOf( 0, 0.4 ) );
        values.add( DataFactory.pairOf( 1, 0.7 ) );
        values.add( DataFactory.pairOf( 0, 0.3 ) );
        values.add( DataFactory.pairOf( 0, 0.5 ) );
        values.add( DataFactory.pairOf( 0, 0.5 ) );
        values.add( DataFactory.pairOf( 0, 0.1 ) );
        values.add( DataFactory.pairOf( 0, 0.1 ) );
        values.add( DataFactory.pairOf( 0, 0.7 ) );
        values.add( DataFactory.pairOf( 0, 0.3 ) );
        values.add( DataFactory.pairOf( 1, 0.2 ) );
        values.add( DataFactory.pairOf( 0, 0.3 ) );
        values.add( DataFactory.pairOf( 0, 0.2 ) );
        values.add( DataFactory.pairOf( 0, 0.2 ) );
        values.add( DataFactory.pairOf( 0, 0.3 ) );
        values.add( DataFactory.pairOf( 0, 0.2 ) );
        values.add( DataFactory.pairOf( 0, 0.5 ) );
        values.add( DataFactory.pairOf( 0, 0.5 ) );
        values.add( DataFactory.pairOf( 0, 0.2 ) );
        values.add( DataFactory.pairOf( 0, 0.3 ) );
        values.add( DataFactory.pairOf( 0, 0.4 ) );
        values.add( DataFactory.pairOf( 0, 0.4 ) );
        values.add( DataFactory.pairOf( 0, 0.4 ) );
        values.add( DataFactory.pairOf( 0, 0.6 ) );
        values.add( DataFactory.pairOf( 1, 0.8 ) );
        values.add( DataFactory.pairOf( 1, 0.8 ) );
        values.add( DataFactory.pairOf( 1, 0.9 ) );
        values.add( DataFactory.pairOf( 0, 0.6 ) );
        values.add( DataFactory.pairOf( 1, 0.5 ) );
        values.add( DataFactory.pairOf( 0, 0.4 ) );
        values.add( DataFactory.pairOf( 0, 0.4 ) );
        values.add( DataFactory.pairOf( 1, 0.6 ) );
        values.add( DataFactory.pairOf( 0, 0.7 ) );
        values.add( DataFactory.pairOf( 1, 0.8 ) );
        values.add( DataFactory.pairOf( 0, 0.3 ) );
        values.add( DataFactory.pairOf( 0, 0.1 ) );
        values.add( DataFactory.pairOf( 1, 0.5 ) );
        values.add( DataFactory.pairOf( 1, 0.3 ) );
        values.add( DataFactory.pairOf( 0, 0.7 ) );
        values.add( DataFactory.pairOf( 0, 0 ) );
        values.add( DataFactory.pairOf( 0, 0.2 ) );
        values.add( DataFactory.pairOf( 0, 0.3 ) );
        values.add( DataFactory.pairOf( 1, 0.7 ) );
        values.add( DataFactory.pairOf( 0, 0.6 ) );
        values.add( DataFactory.pairOf( 0, 0.4 ) );
        values.add( DataFactory.pairOf( 0, 0.2 ) );
        values.add( DataFactory.pairOf( 0, 0 ) );
        values.add( DataFactory.pairOf( 0, 0.2 ) );
        values.add( DataFactory.pairOf( 1, 0.7 ) );
        values.add( DataFactory.pairOf( 0, 0.7 ) );
        values.add( DataFactory.pairOf( 0, 0.5 ) );
        values.add( DataFactory.pairOf( 1, 0.9 ) );
        values.add( DataFactory.pairOf( 1, 0.7 ) );
        values.add( DataFactory.pairOf( 0, 0.4 ) );
        values.add( DataFactory.pairOf( 0, 0.6 ) );
        values.add( DataFactory.pairOf( 0, 0.7 ) );
        values.add( DataFactory.pairOf( 0, 1 ) );
        values.add( DataFactory.pairOf( 1, 0.8 ) );
        values.add( DataFactory.pairOf( 0, 0.3 ) );
        values.add( DataFactory.pairOf( 0, 0.6 ) );
        values.add( DataFactory.pairOf( 0, 0.5 ) );
        values.add( DataFactory.pairOf( 0, 0.1 ) );
        values.add( DataFactory.pairOf( 0, 0.5 ) );
        values.add( DataFactory.pairOf( 0, 0.3 ) );
        values.add( DataFactory.pairOf( 0, 0.1 ) );
        values.add( DataFactory.pairOf( 0, 0.1 ) );
        values.add( DataFactory.pairOf( 0, 0 ) );
        values.add( DataFactory.pairOf( 0, 0 ) );
        values.add( DataFactory.pairOf( 0, 0 ) );
        values.add( DataFactory.pairOf( 0, 0 ) );
        values.add( DataFactory.pairOf( 0, 0 ) );
        values.add( DataFactory.pairOf( 0, 0.2 ) );
        values.add( DataFactory.pairOf( 0, 0.4 ) );
        values.add( DataFactory.pairOf( 0, 0 ) );
        values.add( DataFactory.pairOf( 0, 0.2 ) );
        values.add( DataFactory.pairOf( 0, 0.1 ) );
        values.add( DataFactory.pairOf( 0, 0.3 ) );
        values.add( DataFactory.pairOf( 0, 0.3 ) );
        values.add( DataFactory.pairOf( 0, 0 ) );
        values.add( DataFactory.pairOf( 0, 0.6 ) );
        values.add( DataFactory.pairOf( 0, 0.3 ) );
        values.add( DataFactory.pairOf( 1, 0.9 ) );
        values.add( DataFactory.pairOf( 0, 0.1 ) );
        values.add( DataFactory.pairOf( 0, 0.7 ) );
        values.add( DataFactory.pairOf( 0, 1 ) );
        values.add( DataFactory.pairOf( 0, 0.9 ) );
        values.add( DataFactory.pairOf( 0, 0.2 ) );
        values.add( DataFactory.pairOf( 0, 0.5 ) );
        values.add( DataFactory.pairOf( 1, 0.8 ) );
        values.add( DataFactory.pairOf( 0, 0.3 ) );
        values.add( DataFactory.pairOf( 0, 0.6 ) );
        values.add( DataFactory.pairOf( 0, 0.6 ) );
        values.add( DataFactory.pairOf( 0, 0.7 ) );
        values.add( DataFactory.pairOf( 1, 0.8 ) );
        values.add( DataFactory.pairOf( 1, 1 ) );
        values.add( DataFactory.pairOf( 0, 0.3 ) );
        values.add( DataFactory.pairOf( 0, 0.2 ) );
        values.add( DataFactory.pairOf( 1, 1 ) );
        values.add( DataFactory.pairOf( 1, 0.7 ) );
        values.add( DataFactory.pairOf( 0, 0.4 ) );
        values.add( DataFactory.pairOf( 0, 0 ) );
        values.add( DataFactory.pairOf( 0, 0 ) );
        values.add( DataFactory.pairOf( 0, 0.1 ) );
        values.add( DataFactory.pairOf( 0, 0.1 ) );
        values.add( DataFactory.pairOf( 0, 0.3 ) );
        values.add( DataFactory.pairOf( 0, 0.3 ) );
        values.add( DataFactory.pairOf( 0, 0.2 ) );
        values.add( DataFactory.pairOf( 0, 0.1 ) );
        values.add( DataFactory.pairOf( 0, 0 ) );
        values.add( DataFactory.pairOf( 0, 0.1 ) );
        values.add( DataFactory.pairOf( 1, 0.8 ) );
        values.add( DataFactory.pairOf( 0, 0.8 ) );
        values.add( DataFactory.pairOf( 0, 0.3 ) );
        values.add( DataFactory.pairOf( 0, 0.1 ) );
        values.add( DataFactory.pairOf( 1, 0.5 ) );
        values.add( DataFactory.pairOf( 0, 0.4 ) );
        values.add( DataFactory.pairOf( 1, 0.7 ) );
        values.add( DataFactory.pairOf( 0, 0.3 ) );
        values.add( DataFactory.pairOf( 1, 0.8 ) );
        values.add( DataFactory.pairOf( 0, 0.7 ) );
        values.add( DataFactory.pairOf( 1, 0.7 ) );
        values.add( DataFactory.pairOf( 0, 0.1 ) );
        values.add( DataFactory.pairOf( 0, 0.1 ) );
        values.add( DataFactory.pairOf( 0, 0.1 ) );
        values.add( DataFactory.pairOf( 0, 0 ) );
        values.add( DataFactory.pairOf( 0, 0 ) );
        values.add( DataFactory.pairOf( 0, 0 ) );
        values.add( DataFactory.pairOf( 1, 0.5 ) );
        values.add( DataFactory.pairOf( 0, 0 ) );
        values.add( DataFactory.pairOf( 1, 1 ) );
        values.add( DataFactory.pairOf( 1, 0.7 ) );
        values.add( DataFactory.pairOf( 1, 0.5 ) );
        values.add( DataFactory.pairOf( 0, 0.6 ) );
        values.add( DataFactory.pairOf( 1, 0.4 ) );
        values.add( DataFactory.pairOf( 0, 0.3 ) );
        values.add( DataFactory.pairOf( 0, 0.4 ) );
        values.add( DataFactory.pairOf( 0, 0.3 ) );
        values.add( DataFactory.pairOf( 0, 0 ) );
        values.add( DataFactory.pairOf( 1, 0.8 ) );
        values.add( DataFactory.pairOf( 1, 1 ) );
        values.add( DataFactory.pairOf( 1, 1 ) );
        values.add( DataFactory.pairOf( 0, 0.8 ) );
        values.add( DataFactory.pairOf( 0, 0.5 ) );
        values.add( DataFactory.pairOf( 1, 0.2 ) );
        values.add( DataFactory.pairOf( 0, 0.1 ) );
        values.add( DataFactory.pairOf( 0, 0.1 ) );
        values.add( DataFactory.pairOf( 1, 0.2 ) );
        values.add( DataFactory.pairOf( 0, 0.1 ) );
        values.add( DataFactory.pairOf( 0, 0.1 ) );
        values.add( DataFactory.pairOf( 0, 0.9 ) );
        values.add( DataFactory.pairOf( 0, 0.1 ) );
        values.add( DataFactory.pairOf( 0, 0 ) );
        values.add( DataFactory.pairOf( 0, 0.6 ) );
        values.add( DataFactory.pairOf( 0, 0.7 ) );
        values.add( DataFactory.pairOf( 0, 0.1 ) );
        values.add( DataFactory.pairOf( 0, 0.1 ) );
        values.add( DataFactory.pairOf( 1, 1 ) );
        values.add( DataFactory.pairOf( 1, 0.6 ) );
        values.add( DataFactory.pairOf( 0, 0.2 ) );
        values.add( DataFactory.pairOf( 1, 0.8 ) );
        values.add( DataFactory.pairOf( 0, 0.8 ) );
        values.add( DataFactory.pairOf( 0, 0.6 ) );
        values.add( DataFactory.pairOf( 0, 0.4 ) );
        values.add( DataFactory.pairOf( 1, 0.6 ) );
        values.add( DataFactory.pairOf( 1, 0.3 ) );
        values.add( DataFactory.pairOf( 1, 0.1 ) );
        values.add( DataFactory.pairOf( 1, 0.9 ) );
        values.add( DataFactory.pairOf( 1, 0.7 ) );
        values.add( DataFactory.pairOf( 0, 0.3 ) );
        values.add( DataFactory.pairOf( 1, 0.8 ) );
        values.add( DataFactory.pairOf( 1, 1 ) );
        values.add( DataFactory.pairOf( 1, 0.9 ) );
        values.add( DataFactory.pairOf( 0, 0.1 ) );
        values.add( DataFactory.pairOf( 0, 0.1 ) );
        values.add( DataFactory.pairOf( 0, 0.1 ) );

        final Metadata main = MetadataFactory.getMetadata( MetadataFactory.getDimension(),
                                                           MetadataFactory.getDatasetIdentifier( getLocation( "Tampere" ),
                                                                                                 "MAP",
                                                                                                 "FMI" ) );
        return DataFactory.ofDiscreteProbabilityPairs( values, main );
    }

    /**
     * Returns a set of discrete probability pairs without a baseline and comprising observed non-occurrences only.
     * 
     * @return discrete probability pairs with observed non-occurrences
     */

    public static DiscreteProbabilityPairs getDiscreteProbabilityPairsFour()
    {
        final List<PairOfDoubles> values = new ArrayList<>();
        values.add( DataFactory.pairOf( 0, 3.0 / 5.0 ) );
        values.add( DataFactory.pairOf( 0, 1.0 / 5.0 ) );
        values.add( DataFactory.pairOf( 0, 2.0 / 5.0 ) );
        values.add( DataFactory.pairOf( 0, 3.0 / 5.0 ) );
        values.add( DataFactory.pairOf( 0, 0.0 / 5.0 ) );
        values.add( DataFactory.pairOf( 0, 1.0 / 5.0 ) );

        final Metadata meta = MetadataFactory.getMetadata( MetadataFactory.getDimension(),
                                                           MetadataFactory.getDatasetIdentifier( getLocation( "DRRC2" ),
                                                                                                 "SQIN",
                                                                                                 "HEFS" ) );
        return DataFactory.ofDiscreteProbabilityPairs( values, meta );
    }

    /**
     * Returns a {@link TimeSeriesOfSingleValuedPairs} containing fake data.
     * 
     * @return a time-series of single-valued pairs
     */

    public static TimeSeriesOfSingleValuedPairs getTimeSeriesOfSingleValuedPairsOne()
    {
        // Build an immutable regular time-series of single-valued pairs
        TimeSeriesOfSingleValuedPairsBuilder builder =
                DataFactory.ofTimeSeriesOfSingleValuedPairsBuilder();
        // Create a regular time-series with an issue date/time, a series of paired values, and a timestep
        Instant firstId = Instant.parse( "1985-01-01T00:00:00Z" );
        List<Event<PairOfDoubles>> firstValues = new ArrayList<>();
        // Add some values
        firstValues.add( Event.of( Instant.parse( "1985-01-01T06:00:00Z" ), DataFactory.pairOf( 1, 1 ) ) );
        firstValues.add( Event.of( Instant.parse( "1985-01-01T12:00:00Z" ), DataFactory.pairOf( 1, 5 ) ) );
        firstValues.add( Event.of( Instant.parse( "1985-01-01T18:00:00Z" ), DataFactory.pairOf( 5, 1 ) ) );

        // Add another time-series
        Instant secondId = Instant.parse( "1985-01-02T00:00:00Z" );
        List<Event<PairOfDoubles>> secondValues = new ArrayList<>();
        // Add some values
        secondValues.add( Event.of( Instant.parse( "1985-01-02T06:00:00Z" ), DataFactory.pairOf( 10, 1 ) ) );
        secondValues.add( Event.of( Instant.parse( "1985-01-02T12:00:00Z" ), DataFactory.pairOf( 1, 1 ) ) );
        secondValues.add( Event.of( Instant.parse( "1985-01-02T18:00:00Z" ), DataFactory.pairOf( 1, 10 ) ) );

        // Create some default metadata for the time-series
        final TimeWindow window = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                 Instant.parse( "1985-01-02T00:00:00Z" ),
                                                 ReferenceTime.ISSUE_TIME,
                                                 Duration.ofHours( 6 ),
                                                 Duration.ofHours( 18 ) );
        final Metadata metaData = MetadataFactory.getMetadata( MetadataFactory.getDimension( "CMS" ),
                                                               MetadataFactory.getDatasetIdentifier( getLocation( "A" ),
                                                                                                     "Streamflow" ),
                                                               window );
        // Build the time-series
        return (TimeSeriesOfSingleValuedPairs) builder.addTimeSeriesData( firstId, firstValues )
                                                      .addTimeSeriesData( secondId, secondValues )
                                                      .setMetadata( metaData )
                                                      .build();
    }

    /**
     * Returns a {@link TimeSeriesOfSingleValuedPairs} containing fake data.
     * 
     * @return a time-series of single-valued pairs
     */

    public static TimeSeriesOfSingleValuedPairs getTimeSeriesOfSingleValuedPairsTwo()
    {
        // Build an immutable regular time-series of single-valued pairs
        TimeSeriesOfSingleValuedPairsBuilder builder =
                DataFactory.ofTimeSeriesOfSingleValuedPairsBuilder();
        // Create a regular time-series with an issue date/time, a series of paired values, and a timestep
        Instant firstId = Instant.parse( "1985-01-01T00:00:00Z" );
        List<Event<PairOfDoubles>> firstValues = new ArrayList<>();
        // Add some values
        firstValues.add( Event.of( Instant.parse( "1985-01-01T06:00:00Z" ), DataFactory.pairOf( 1, 1 ) ) );
        firstValues.add( Event.of( Instant.parse( "1985-01-01T12:00:00Z" ), DataFactory.pairOf( 1, 5 ) ) );
        firstValues.add( Event.of( Instant.parse( "1985-01-01T18:00:00Z" ), DataFactory.pairOf( 5, 1 ) ) );

        // Create some default metadata for the time-series
        final TimeWindow window = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                 Instant.parse( "1985-01-01T00:00:00Z" ),
                                                 ReferenceTime.ISSUE_TIME,
                                                 Duration.ofHours( 6 ),
                                                 Duration.ofHours( 18 ) );
        final Metadata metaData = MetadataFactory.getMetadata( MetadataFactory.getDimension( "CMS" ),
                                                               MetadataFactory.getDatasetIdentifier( getLocation( "A" ),
                                                                                                     "Streamflow" ),
                                                               window );
        // Build the time-series
        return (TimeSeriesOfSingleValuedPairs) builder.addTimeSeriesData( firstId, firstValues )
                                                      .setMetadata( metaData )
                                                      .build();
    }

    /**
     * Returns a {@link TimeSeriesOfSingleValuedPairs} containing fake data.
     * 
     * @return a time-series of single-valued pairs
     */

    public static TimeSeriesOfSingleValuedPairs getTimeSeriesOfSingleValuedPairsThree()
    {
        // Build an immutable regular time-series of single-valued pairs
        TimeSeriesOfSingleValuedPairsBuilder builder =
                DataFactory.ofTimeSeriesOfSingleValuedPairsBuilder();
        // Create a regular time-series with an issue date/time, a series of paired values, and a timestep

        // Add another time-series
        Instant secondId = Instant.parse( "1985-01-02T00:00:00Z" );
        List<Event<PairOfDoubles>> secondValues = new ArrayList<>();
        // Add some values
        secondValues.add( Event.of( Instant.parse( "1985-01-02T06:00:00Z" ), DataFactory.pairOf( 10, 1 ) ) );
        secondValues.add( Event.of( Instant.parse( "1985-01-02T12:00:00Z" ), DataFactory.pairOf( 1, 1 ) ) );
        secondValues.add( Event.of( Instant.parse( "1985-01-02T18:00:00Z" ), DataFactory.pairOf( 1, 10 ) ) );

        // Create some default metadata for the time-series
        final TimeWindow window = TimeWindow.of( Instant.parse( "1985-01-02T00:00:00Z" ),
                                                 Instant.parse( "1985-01-02T00:00:00Z" ),
                                                 ReferenceTime.ISSUE_TIME,
                                                 Duration.ofHours( 6 ),
                                                 Duration.ofHours( 18 ) );
        final Metadata metaData = MetadataFactory.getMetadata( MetadataFactory.getDimension( "CMS" ),
                                                               MetadataFactory.getDatasetIdentifier( getLocation( "A" ),
                                                                                                     "Streamflow" ),
                                                               window );
        // Build the time-series
        return (TimeSeriesOfSingleValuedPairs) builder.addTimeSeriesData( secondId, secondValues )
                                                      .setMetadata( metaData )
                                                      .build();
    }

    /**
     * Returns a {@link TimeSeriesOfSingleValuedPairs} containing no data.
     * 
     * @return a time-series of single-valued pairs
     */

    public static TimeSeriesOfSingleValuedPairs getTimeSeriesOfSingleValuedPairsFour()
    {
        // Build an immutable regular time-series of single-valued pairs
        TimeSeriesOfSingleValuedPairsBuilder builder =
                DataFactory.ofTimeSeriesOfSingleValuedPairsBuilder();
        // Create a regular time-series with an issue date/time, a series of paired values, and a timestep

        // Create some default metadata for the time-series
        final TimeWindow window = TimeWindow.of( Instant.MIN,
                                                 Instant.MAX );
        final Metadata metaData = MetadataFactory.getMetadata( MetadataFactory.getDimension( "CMS" ),
                                                               MetadataFactory.getDatasetIdentifier( getLocation( "A" ),
                                                                                                     "Streamflow" ),
                                                               window );
        // Build the time-series
        return (TimeSeriesOfSingleValuedPairs) builder.setMetadata( metaData )
                                                      .build();
    }


    /**
     * Returns a {@link TimeSeriesOfSingleValuedPairs} containing fake data with the same peak at multiple times.
     * 
     * @return a time-series of single-valued pairs
     */

    public static TimeSeriesOfSingleValuedPairs getTimeSeriesOfSingleValuedPairsFive()
    {
        // Build an immutable regular time-series of single-valued pairs
        TimeSeriesOfSingleValuedPairsBuilder builder =
                DataFactory.ofTimeSeriesOfSingleValuedPairsBuilder();
        // Create a regular time-series with an issue date/time, a series of paired values, and a timestep

        // Add another time-series
        Instant secondId = Instant.parse( "1985-01-02T00:00:00Z" );
        List<Event<PairOfDoubles>> secondValues = new ArrayList<>();

        // Add some values
        secondValues.add( Event.of( Instant.parse( "1985-01-02T06:00:00Z" ), DataFactory.pairOf( 10, 1 ) ) );
        secondValues.add( Event.of( Instant.parse( "1985-01-02T12:00:00Z" ), DataFactory.pairOf( 1, 1 ) ) );
        secondValues.add( Event.of( Instant.parse( "1985-01-02T18:00:00Z" ), DataFactory.pairOf( 10, 10 ) ) );
        secondValues.add( Event.of( Instant.parse( "1985-01-03T00:00:00Z" ), DataFactory.pairOf( 2, 10 ) ) );
        secondValues.add( Event.of( Instant.parse( "1985-01-03T06:00:00Z" ), DataFactory.pairOf( 4, 7 ) ) );

        // Create some default metadata for the time-series
        final TimeWindow window = TimeWindow.of( Instant.parse( "1985-01-02T00:00:00Z" ),
                                                 Instant.parse( "1985-01-02T00:00:00Z" ),
                                                 ReferenceTime.ISSUE_TIME,
                                                 Duration.ofHours( 6 ),
                                                 Duration.ofHours( 30 ) );
        final Metadata metaData = MetadataFactory.getMetadata( MetadataFactory.getDimension( "CMS" ),
                                                               MetadataFactory.getDatasetIdentifier( getLocation( "A" ),
                                                                                                     "Streamflow" ),
                                                               window );
        // Build the time-series
        return (TimeSeriesOfSingleValuedPairs) builder.addTimeSeriesData( secondId, secondValues )
                                                      .setMetadata( metaData )
                                                      .build();
    }

}
