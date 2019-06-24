package wres.engine.statistics.metric;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import wres.datamodel.VectorOfDoubles;
import wres.datamodel.sampledata.DatasetIdentifier;
import wres.datamodel.sampledata.Location;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.sampledata.SampleMetadata.SampleMetadataBuilder;
import wres.datamodel.sampledata.pairs.DichotomousPair;
import wres.datamodel.sampledata.pairs.DichotomousPairs;
import wres.datamodel.sampledata.pairs.DiscreteProbabilityPair;
import wres.datamodel.sampledata.pairs.DiscreteProbabilityPairs;
import wres.datamodel.sampledata.pairs.EnsemblePair;
import wres.datamodel.sampledata.pairs.EnsemblePairs;
import wres.datamodel.sampledata.pairs.MulticategoryPair;
import wres.datamodel.sampledata.pairs.MulticategoryPairs;
import wres.datamodel.sampledata.pairs.SingleValuedPair;
import wres.datamodel.sampledata.pairs.SingleValuedPairs;
import wres.datamodel.sampledata.pairs.TimeSeriesOfSingleValuedPairs;
import wres.datamodel.sampledata.pairs.TimeSeriesOfSingleValuedPairs.TimeSeriesOfSingleValuedPairsBuilder;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeWindow;

/**
 * Factory class for generating test datasets for metric calculations.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class MetricTestDataFactory
{

    /**
     * Streamflow for metadata.
     */
    private static final String STREAMFLOW = "Streamflow";
    
    /**
     * Units for metadata.
     */
    
    private static final String MM_DAY = "MM/DAY";
    
    /**
     * Location for metadata.
     */
    
    private static final String DRRC2 = "DRRC2";
    
    /**
     * Fourteenth time.
     */
    
    private static final String FOURTEENTH_TIME = "2551-03-19T09:00:00Z";
    
    /**
     * Thirteenth time.
     */
    
    private static final String THIRTEENTH_TIME = "2551-03-19T06:00:00Z";
    
    /**
     * Twelfth time.
     */
    
    private static final String TWELFTH_TIME = "2551-03-19T03:00:00Z";
    
    /**
     * Eleventh time.
     */
    
    private static final String ELEVENTH_TIME = "2551-03-19T00:00:00Z";
    
    /**
     * Tenth time.
     */
    
    private static final String TENTH_TIME = "2551-03-18T21:00:00Z";
    
    /**
     * Ninth time.
     */
    
    private static final String NINTH_TIME = "2551-03-18T18:00:00Z";
    
    /**
     * Eight time.
     */
    
    private static final String EIGHTH_TIME = "2551-03-18T15:00:00Z";
    
    /**
     * Seventh time.
     */
    
    private static final String SEVENTH_TIME = "2551-03-18T12:00:00Z";
    
    /**
     * Sixth time.
     */
    
    private static final String SIXTH_TIME = "1985-01-02T18:00:00Z";
    
    /**
     * Fifth time.
     */
    
    private static final String FIFTH_TIME = "1985-01-02T12:00:00Z";
    
    /**
     * Fourth time.
     */
    
    private static final String FOURTH_TIME = "1985-01-02T06:00:00Z";
    
    /**
     * Third time.
     */
    
    private static final String THIRD_TIME = "1985-01-02T00:00:00Z";
    
    /**
     * Second time.
     */
    
    private static final String SECOND_TIME = "2010-12-31T11:59:59Z";
    
    /**
     * First time.
     */
    
    private static final String FIRST_TIME = "1985-01-01T00:00:00Z";

    /**
     * Returns a set of single-valued pairs without a baseline.
     * 
     * @return single-valued pairs
     */

    public static SingleValuedPairs getSingleValuedPairsOne()
    {
        //Construct some single-valued pairs
        final List<SingleValuedPair> values = new ArrayList<>();
        values.add( SingleValuedPair.of( 22.9, 22.8 ) );
        values.add( SingleValuedPair.of( 75.2, 80 ) );
        values.add( SingleValuedPair.of( 63.2, 65 ) );
        values.add( SingleValuedPair.of( 29, 30 ) );
        values.add( SingleValuedPair.of( 5, 2 ) );
        values.add( SingleValuedPair.of( 2.1, 3.1 ) );
        values.add( SingleValuedPair.of( 35000, 37000 ) );
        values.add( SingleValuedPair.of( 8, 7 ) );
        values.add( SingleValuedPair.of( 12, 12 ) );
        values.add( SingleValuedPair.of( 93, 94 ) );

        return SingleValuedPairs.of( values, SampleMetadata.of() );
    }

    /**
     * Returns a set of single-valued pairs with a baseline.
     * 
     * @return single-valued pairs
     */

    public static SingleValuedPairs getSingleValuedPairsTwo()
    {
        //Construct some single-valued pairs
        final List<SingleValuedPair> values = new ArrayList<>();
        values.add( SingleValuedPair.of( 22.9, 22.8 ) );
        values.add( SingleValuedPair.of( 75.2, 80 ) );
        values.add( SingleValuedPair.of( 63.2, 65 ) );
        values.add( SingleValuedPair.of( 29, 30 ) );
        values.add( SingleValuedPair.of( 5, 2 ) );
        values.add( SingleValuedPair.of( 2.1, 3.1 ) );
        values.add( SingleValuedPair.of( 35000, 37000 ) );
        values.add( SingleValuedPair.of( 8, 7 ) );
        values.add( SingleValuedPair.of( 12, 12 ) );
        values.add( SingleValuedPair.of( 93, 94 ) );
        final List<SingleValuedPair> baseline = new ArrayList<>();
        baseline.add( SingleValuedPair.of( 20.9, 23.8 ) );
        baseline.add( SingleValuedPair.of( 71.2, 83.2 ) );
        baseline.add( SingleValuedPair.of( 69.2, 66 ) );
        baseline.add( SingleValuedPair.of( 20, 30.5 ) );
        baseline.add( SingleValuedPair.of( 5.8, 2.1 ) );
        baseline.add( SingleValuedPair.of( 1.1, 3.4 ) );
        baseline.add( SingleValuedPair.of( 33020, 37500 ) );
        baseline.add( SingleValuedPair.of( 8.8, 7.1 ) );
        baseline.add( SingleValuedPair.of( 12.1, 13 ) );
        baseline.add( SingleValuedPair.of( 93.2, 94.8 ) );

        final SampleMetadata main = SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                       DatasetIdentifier.of( getLocation( DRRC2 ),
                                                                             "SQIN",
                                                                             "HEFS" ) );
        final SampleMetadata base = SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                       DatasetIdentifier.of( getLocation( DRRC2 ),
                                                                             "SQIN",
                                                                             "ESP" ) );
        return SingleValuedPairs.of( values, baseline, main, base );
    }

    public static Location getLocation( final String locationId )
    {
        return Location.of( locationId );
    }

    /**
     * Returns a moderately-sized (10k) test dataset of single-valued pairs, {5,10}, without a baseline.
     * 
     * @return single-valued pairs
     */

    public static SingleValuedPairs getSingleValuedPairsThree()
    {
        //Construct some single-valued pairs
        final List<SingleValuedPair> values = new ArrayList<>();

        for ( int i = 0; i < 10000; i++ )
        {
            values.add( SingleValuedPair.of( 5, 10 ) );
        }

        final SampleMetadata meta = SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                       DatasetIdentifier.of( getLocation( DRRC2 ),
                                                                             "SQIN",
                                                                             "HEFS" ) );
        return SingleValuedPairs.of( values, meta );
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
        final List<SingleValuedPair> values = new ArrayList<>();

        for ( int i = 0; i < 5; i++ )
        {
            for ( int j = 0; j < 100; j++ )
            {
                values.add( SingleValuedPair.of( i + 1.0, i + 6.0 ) );
            }
        }

        final TimeWindow window = TimeWindow.of( Instant.parse( FIRST_TIME ),
                                                 Instant.parse( SECOND_TIME ),
                                                 Duration.ofHours( 1 ) );
        final SampleMetadata meta = new SampleMetadataBuilder().setMeasurementUnit( MeasurementUnit.of( "CMS" ) )
                                                               .setIdentifier( DatasetIdentifier.of( getLocation( DRRC2 ),
                                                                                                     "SQIN",
                                                                                                     "HEFS" ) )
                                                               .setTimeWindow( window )
                                                               .build();
        return SingleValuedPairs.of( values, meta );
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
        final List<SingleValuedPair> values = new ArrayList<>();

        File file = new File( "testinput/metricTestDataFactory/getSingleValuedPairsFive.asc" );
        try ( BufferedReader in =
                new BufferedReader( new InputStreamReader( new FileInputStream( file ), StandardCharsets.UTF_8 ) ) )
        {
            String line = null;
            while ( Objects.nonNull( line = in.readLine() ) && !line.isEmpty() )
            {
                double[] doubleValues =
                        Arrays.stream( line.split( "\\s+" ) ).mapToDouble( Double::parseDouble ).toArray();
                values.add( SingleValuedPair.of( doubleValues[0], doubleValues[1] ) );
            }
        }

        final TimeWindow window = TimeWindow.of( Instant.parse( FIRST_TIME ),
                                                 Instant.parse( SECOND_TIME ),
                                                 Duration.ofHours( 24 ) );
        final SampleMetadata meta = new SampleMetadataBuilder().setMeasurementUnit( MeasurementUnit.of( MM_DAY ) )
                                                               .setIdentifier( DatasetIdentifier.of( getLocation( "103.1" ),
                                                                                                     "QME",
                                                                                                     "NVE" ) )
                                                               .setTimeWindow( window )
                                                               .build();
        return SingleValuedPairs.of( values, meta );
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

        final List<SingleValuedPair> values = new ArrayList<>();
        values.add( SingleValuedPair.of( 22.9, 22.8 ) );
        final TimeWindow window = TimeWindow.of( Instant.parse( FIRST_TIME ),
                                                 Instant.parse( SECOND_TIME ),
                                                 Duration.ofHours( 24 ) );
        final SampleMetadata meta = new SampleMetadataBuilder().setMeasurementUnit( MeasurementUnit.of( MM_DAY ) )
                                                               .setIdentifier( DatasetIdentifier.of( getLocation( "A" ),
                                                                                                     "MAP" ) )
                                                               .setTimeWindow( window )
                                                               .build();
        return SingleValuedPairs.of( values, meta );
    }

    /**
     * Returns a set of single-valued pairs with a baseline, both empty.
     * 
     * @return single-valued pairs
     */

    public static SingleValuedPairs getSingleValuedPairsSeven()
    {
        //Construct some single-valued pairs
        final SampleMetadata main = SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                       DatasetIdentifier.of( getLocation( DRRC2 ),
                                                                             "SQIN",
                                                                             "HEFS" ) );
        final SampleMetadata base = SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                       DatasetIdentifier.of( getLocation( DRRC2 ),
                                                                             "SQIN",
                                                                             "ESP" ) );
        return SingleValuedPairs.of( Collections.emptyList(), Collections.emptyList(), main, base );
    }

    /**
     * Returns a set of single-valued pairs without a baseline and with some missing values.
     * 
     * @return single-valued pairs
     */

    public static SingleValuedPairs getSingleValuedPairsEight()
    {
        //Construct some single-valued pairs
        final List<SingleValuedPair> values = new ArrayList<>();
        values.add( SingleValuedPair.of( 22.9, 22.8 ) );
        values.add( SingleValuedPair.of( 75.2, 80 ) );
        values.add( SingleValuedPair.of( 63.2, 65 ) );
        values.add( SingleValuedPair.of( 29, 30 ) );
        values.add( SingleValuedPair.of( 5, 2 ) );
        values.add( SingleValuedPair.of( 2.1, 3.1 ) );
        values.add( SingleValuedPair.of( 35000, 37000 ) );
        values.add( SingleValuedPair.of( 8, 7 ) );
        values.add( SingleValuedPair.of( 12, 12 ) );
        values.add( SingleValuedPair.of( 93, 94 ) );
        values.add( SingleValuedPair.of( Double.NaN, 94 ) );
        values.add( SingleValuedPair.of( 93, Double.NaN ) );
        values.add( SingleValuedPair.of( Double.NaN, Double.NaN ) );

        return SingleValuedPairs.of( values, SampleMetadata.of() );
    }

    /**
     * Returns a list of {@link TimeSeriesOfSingleValuedPairs} which corresponds to the pairs 
     * associated with system test scenario504 as of commit e91b36a8f6b798d1987e78a0f37b38f3ca4501ae.
     * The pairs are reproduced to 2 d.p. only.
     * 
     * @return a time series of single-valued pairs
     */

    public static TimeSeriesOfSingleValuedPairs getSingleValuedPairsNine()
    {
        TimeSeriesOfSingleValuedPairsBuilder tsBuilder = new TimeSeriesOfSingleValuedPairsBuilder();

        List<Event<SingleValuedPair>> listOfPairs = new ArrayList<>();

        // Add the first time-series
        Instant basisTime = Instant.parse( "2551-03-17T12:00:00Z" );

        listOfPairs.add( Event.of( basisTime,
                                   Instant.parse( "2551-03-17T15:00:00Z" ),
                                   SingleValuedPair.of( 409.67, 73.00 ) ) );
        listOfPairs.add( Event.of( basisTime,
                                   Instant.parse( "2551-03-17T18:00:00Z" ),
                                   SingleValuedPair.of( 428.33, 79.00 ) ) );
        listOfPairs.add( Event.of( basisTime,
                                   Instant.parse( "2551-03-17T21:00:00Z" ),
                                   SingleValuedPair.of( 443.67, 83.00 ) ) );
        listOfPairs.add( Event.of( basisTime,
                                   Instant.parse( "2551-03-18T00:00:00Z" ),
                                   SingleValuedPair.of( 460.33, 89.00 ) ) );
        listOfPairs.add( Event.of( basisTime,
                                   Instant.parse( "2551-03-18T03:00:00Z" ),
                                   SingleValuedPair.of( 477.67, 97.00 ) ) );
        listOfPairs.add( Event.of( basisTime,
                                   Instant.parse( "2551-03-18T06:00:00Z" ),
                                   SingleValuedPair.of( 497.67, 101.00 ) ) );
        listOfPairs.add( Event.of( basisTime,
                                   Instant.parse( "2551-03-18T09:00:00Z" ),
                                   SingleValuedPair.of( 517.67, 103.00 ) ) );
        listOfPairs.add( Event.of( basisTime,
                                   Instant.parse( SEVENTH_TIME ),
                                   SingleValuedPair.of( 548.33, 107.00 ) ) );
        listOfPairs.add( Event.of( basisTime,
                                   Instant.parse( EIGHTH_TIME ),
                                   SingleValuedPair.of( 567.67, 109.00 ) ) );
        listOfPairs.add( Event.of( basisTime,
                                   Instant.parse( NINTH_TIME ),
                                   SingleValuedPair.of( 585.67, 113.00 ) ) );
        listOfPairs.add( Event.of( basisTime,
                                   Instant.parse( TENTH_TIME ),
                                   SingleValuedPair.of( 602.33, 127.00 ) ) );

        // Add second time-series
        Instant basisTimeTwo = Instant.parse( "2551-03-18T00:00:00Z" );
        listOfPairs.add( Event.of( basisTimeTwo,
                                   Instant.parse( "2551-03-18T03:00:00Z" ),
                                   SingleValuedPair.of( 477.67, 131.00 ) ) );
        listOfPairs.add( Event.of( basisTimeTwo,
                                   Instant.parse( "2551-03-18T06:00:00Z" ),
                                   SingleValuedPair.of( 497.67, 137.00 ) ) );
        listOfPairs.add( Event.of( basisTimeTwo,
                                   Instant.parse( "2551-03-18T09:00:00Z" ),
                                   SingleValuedPair.of( 517.67, 139.00 ) ) );
        listOfPairs.add( Event.of( basisTimeTwo,
                                   Instant.parse( SEVENTH_TIME ),
                                   SingleValuedPair.of( 548.33, 149.00 ) ) );
        listOfPairs.add( Event.of( basisTimeTwo,
                                   Instant.parse( EIGHTH_TIME ),
                                   SingleValuedPair.of( 567.67, 151.00 ) ) );
        listOfPairs.add( Event.of( basisTimeTwo,
                                   Instant.parse( NINTH_TIME ),
                                   SingleValuedPair.of( 585.67, 157.00 ) ) );
        listOfPairs.add( Event.of( basisTimeTwo,
                                   Instant.parse( TENTH_TIME ),
                                   SingleValuedPair.of( 602.33, 163.00 ) ) );
        listOfPairs.add( Event.of( basisTimeTwo,
                                   Instant.parse( ELEVENTH_TIME ),
                                   SingleValuedPair.of( 616.33, 167.00 ) ) );
        listOfPairs.add( Event.of( basisTimeTwo,
                                   Instant.parse( TWELFTH_TIME ),
                                   SingleValuedPair.of( 638.33, 173.00 ) ) );
        listOfPairs.add( Event.of( basisTimeTwo,
                                   Instant.parse( THIRTEENTH_TIME ),
                                   SingleValuedPair.of( 653.00, 179.00 ) ) );
        listOfPairs.add( Event.of( basisTimeTwo,
                                   Instant.parse( FOURTEENTH_TIME ),
                                   SingleValuedPair.of( 670.33, 181.00 ) ) );

        // Add third time-series
        Instant basisTimeThree = Instant.parse( SEVENTH_TIME );
        listOfPairs.add( Event.of( basisTimeThree,
                                   Instant.parse( EIGHTH_TIME ),
                                   SingleValuedPair.of( 567.67, 191.00 ) ) );
        listOfPairs.add( Event.of( basisTimeThree,
                                   Instant.parse( NINTH_TIME ),
                                   SingleValuedPair.of( 585.67, 193.00 ) ) );
        listOfPairs.add( Event.of( basisTimeThree,
                                   Instant.parse( TENTH_TIME ),
                                   SingleValuedPair.of( 602.33, 197.00 ) ) );
        listOfPairs.add( Event.of( basisTimeThree,
                                   Instant.parse( ELEVENTH_TIME ),
                                   SingleValuedPair.of( 616.33, 199.00 ) ) );
        listOfPairs.add( Event.of( basisTimeThree,
                                   Instant.parse( TWELFTH_TIME ),
                                   SingleValuedPair.of( 638.33, 211.00 ) ) );
        listOfPairs.add( Event.of( basisTimeThree,
                                   Instant.parse( THIRTEENTH_TIME ),
                                   SingleValuedPair.of( 653.00, 223.00 ) ) );
        listOfPairs.add( Event.of( basisTimeThree,
                                   Instant.parse( FOURTEENTH_TIME ),
                                   SingleValuedPair.of( 670.33, 227.00 ) ) );
        listOfPairs.add( Event.of( basisTimeThree,
                                   Instant.parse( "2551-03-19T12:00:00Z" ),
                                   SingleValuedPair.of( 691.67, 229.00 ) ) );
        listOfPairs.add( Event.of( basisTimeThree,
                                   Instant.parse( "2551-03-19T15:00:00Z" ),
                                   SingleValuedPair.of( 718.33, 233.00 ) ) );
        listOfPairs.add( Event.of( basisTimeThree,
                                   Instant.parse( "2551-03-19T18:00:00Z" ),
                                   SingleValuedPair.of( 738.33, 239.00 ) ) );
        listOfPairs.add( Event.of( basisTimeThree,
                                   Instant.parse( "2551-03-19T21:00:00Z" ),
                                   SingleValuedPair.of( 756.33, 241.00 ) ) );

        // Add third time-series
        Instant basisTimeFour = Instant.parse( ELEVENTH_TIME );
        listOfPairs.add( Event.of( basisTimeFour,
                                   Instant.parse( TWELFTH_TIME ),
                                   SingleValuedPair.of( 638.33, 251.00 ) ) );
        listOfPairs.add( Event.of( basisTimeFour,
                                   Instant.parse( THIRTEENTH_TIME ),
                                   SingleValuedPair.of( 653.00, 257.00 ) ) );
        listOfPairs.add( Event.of( basisTimeFour,
                                   Instant.parse( FOURTEENTH_TIME ),
                                   SingleValuedPair.of( 670.33, 263.00 ) ) );
        listOfPairs.add( Event.of( basisTimeFour,
                                   Instant.parse( "2551-03-19T12:00:00Z" ),
                                   SingleValuedPair.of( 691.67, 269.00 ) ) );
        listOfPairs.add( Event.of( basisTimeFour,
                                   Instant.parse( "2551-03-19T15:00:00Z" ),
                                   SingleValuedPair.of( 718.33, 271.00 ) ) );
        listOfPairs.add( Event.of( basisTimeFour,
                                   Instant.parse( "2551-03-19T18:00:00Z" ),
                                   SingleValuedPair.of( 738.33, 277.00 ) ) );
        listOfPairs.add( Event.of( basisTimeFour,
                                   Instant.parse( "2551-03-19T21:00:00Z" ),
                                   SingleValuedPair.of( 756.33, 281.00 ) ) );
        listOfPairs.add( Event.of( basisTimeFour,
                                   Instant.parse( "2551-03-20T00:00:00Z" ),
                                   SingleValuedPair.of( 776.33, 283.00 ) ) );
        listOfPairs.add( Event.of( basisTimeFour,
                                   Instant.parse( "2551-03-20T03:00:00Z" ),
                                   SingleValuedPair.of( 805.67, 293.00 ) ) );
        listOfPairs.add( Event.of( basisTimeFour,
                                   Instant.parse( "2551-03-20T06:00:00Z" ),
                                   SingleValuedPair.of( 823.67, 307.00 ) ) );
        listOfPairs.add( Event.of( basisTimeFour,
                                   Instant.parse( "2551-03-20T09:00:00Z" ),
                                   SingleValuedPair.of( 840.33, 311.00 ) ) );

        final TimeWindow window = TimeWindow.of( Instant.parse( "2551-03-17T00:00:00Z" ),
                                                 Instant.parse( "2551-03-20T00:00:00Z" ),
                                                 Duration.ofSeconds( 10800 ),
                                                 Duration.ofSeconds( 118800 ) );

        SampleMetadata metaData = new SampleMetadataBuilder().setMeasurementUnit( MeasurementUnit.of( "CMS" ) )
                                                             .setIdentifier( DatasetIdentifier.of( getLocation( "FAKE2" ),
                                                                                                   "DISCHARGE" ) )
                                                             .setTimeWindow( window )
                                                             .build();

        tsBuilder.setMetadata( metaData );

        return tsBuilder.addTimeSeries( listOfPairs ).build();
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
        final List<EnsemblePair> values = new ArrayList<>();

        File file = new File( "testinput/metricTestDataFactory/getEnsemblePairsOne.asc" );
        List<Double> climatology = new ArrayList<>();
        try ( BufferedReader in =
                new BufferedReader( new InputStreamReader( new FileInputStream( file ), StandardCharsets.UTF_8 ) ) )
        {
            String line = null;
            while ( Objects.nonNull( line = in.readLine() ) && !line.isEmpty() )
            {
                double[] doubleValues =
                        Arrays.stream( line.split( "\\s+" ) ).mapToDouble( Double::parseDouble ).toArray();
                values.add( EnsemblePair.of( doubleValues[0],
                                             Arrays.copyOfRange( doubleValues, 1, doubleValues.length ) ) );
                climatology.add( doubleValues[0] );
            }
        }

        final TimeWindow window = TimeWindow.of( Instant.parse( FIRST_TIME ),
                                                 Instant.parse( SECOND_TIME ),
                                                 Duration.ofHours( 24 ) );
        final SampleMetadata meta = new SampleMetadataBuilder().setMeasurementUnit( MeasurementUnit.of( "CMS" ) )
                                                               .setIdentifier( DatasetIdentifier.of( getLocation( DRRC2 ),
                                                                                                     "SQIN",
                                                                                                     "HEFS" ) )
                                                               .setTimeWindow( window )
                                                               .build();
        final SampleMetadata baseMeta = new SampleMetadataBuilder().setMeasurementUnit( MeasurementUnit.of( "CMS" ) )
                                                                   .setIdentifier( DatasetIdentifier.of( getLocation( DRRC2 ),
                                                                                                         "SQIN",
                                                                                                         "ESP" ) )
                                                                   .setTimeWindow( window )
                                                                   .build();
        return EnsemblePairs.of( values,
                                 values,
                                 meta,
                                 baseMeta,
                                 VectorOfDoubles.of( climatology.toArray( new Double[climatology.size()] ) ) );
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
        final List<EnsemblePair> values = new ArrayList<>();

        File file = new File( "testinput/metricTestDataFactory/getEnsemblePairsOne.asc" );
        List<Double> climatology = new ArrayList<>();
        try ( BufferedReader in =
                new BufferedReader( new InputStreamReader( new FileInputStream( file ), StandardCharsets.UTF_8 ) ) )
        {
            String line = null;
            while ( Objects.nonNull( line = in.readLine() ) && !line.isEmpty() )
            {
                double[] doubleValues =
                        Arrays.stream( line.split( "\\s+" ) ).mapToDouble( Double::parseDouble ).toArray();
                values.add( EnsemblePair.of( doubleValues[0],
                                             Arrays.copyOfRange( doubleValues, 1, doubleValues.length ) ) );
                climatology.add( doubleValues[0] );
            }
        }
        //Add some missing values
        climatology.add( Double.NaN );
        values.add( EnsemblePair.of( Double.NaN,
                                     new double[] { Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN } ) );

        final TimeWindow window = TimeWindow.of( Instant.parse( FIRST_TIME ),
                                                 Instant.parse( SECOND_TIME ),
                                                 Duration.ofHours( 24 ) );
        final SampleMetadata meta = new SampleMetadataBuilder().setMeasurementUnit( MeasurementUnit.of( "CMS" ) )
                                                               .setIdentifier( DatasetIdentifier.of( getLocation( DRRC2 ),
                                                                                                     "SQIN",
                                                                                                     "HEFS" ) )
                                                               .setTimeWindow( window )
                                                               .build();

        final SampleMetadata baseMeta = new SampleMetadataBuilder().setMeasurementUnit( MeasurementUnit.of( "CMS" ) )
                                                                   .setIdentifier( DatasetIdentifier.of( getLocation( DRRC2 ),
                                                                                                         "SQIN",
                                                                                                         "ESP" ) )
                                                                   .setTimeWindow( window )
                                                                   .build();

        return EnsemblePairs.of( values,
                                 values,
                                 meta,
                                 baseMeta,
                                 VectorOfDoubles.of( climatology.toArray( new Double[climatology.size()] ) ) );
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
        final List<EnsemblePair> values = new ArrayList<>();

        File file = new File( "testinput/metricTestDataFactory/getEnsemblePairsTwo.asc" );
        List<Double> climatology = new ArrayList<>();
        try ( BufferedReader in =
                new BufferedReader( new InputStreamReader( new FileInputStream( file ), StandardCharsets.UTF_8 ) ) )
        {
            String line = null;
            while ( Objects.nonNull( line = in.readLine() ) && !line.isEmpty() )
            {
                double[] doubleValues =
                        Arrays.stream( line.split( "\\s+" ) ).mapToDouble( Double::parseDouble ).toArray();
                values.add( EnsemblePair.of( doubleValues[0],
                                             Arrays.copyOfRange( doubleValues, 1, doubleValues.length ) ) );
                climatology.add( doubleValues[0] );
            }
        }

        final TimeWindow window = TimeWindow.of( Instant.parse( FIRST_TIME ),
                                                 Instant.parse( SECOND_TIME ),
                                                 Duration.ofHours( 24 ) );
        final SampleMetadata meta = new SampleMetadataBuilder().setMeasurementUnit( MeasurementUnit.of( "CMS" ) )
                                                               .setIdentifier( DatasetIdentifier.of( getLocation( DRRC2 ),
                                                                                                     "SQIN",
                                                                                                     "HEFS" ) )
                                                               .setTimeWindow( window )
                                                               .build();
        return EnsemblePairs.of( values,
                                 meta,
                                 VectorOfDoubles.of( climatology.toArray( new Double[climatology.size()] ) ) );
    }

    /**
     * Returns a set of ensemble pairs with a single pair and no baseline. 
     * 
     * @return ensemble pairs
     */

    public static EnsemblePairs getEnsemblePairsThree()
    {
        //Construct some ensemble pairs

        final List<EnsemblePair> values = new ArrayList<>();
        values.add( EnsemblePair.of( 22.9, new double[] { 22.8, 23.9 } ) );

        final TimeWindow window = TimeWindow.of( Instant.parse( FIRST_TIME ),
                                                 Instant.parse( SECOND_TIME ),
                                                 Duration.ofHours( 24 ) );
        final SampleMetadata meta = new SampleMetadataBuilder().setMeasurementUnit( MeasurementUnit.of( MM_DAY ) )
                                                               .setIdentifier( DatasetIdentifier.of( getLocation( DRRC2 ),
                                                                                                     "MAP" ) )
                                                               .setTimeWindow( window )
                                                               .build();
        return EnsemblePairs.of( values, meta );
    }

    /**
     * Returns a set of ensemble pairs with no data in the main input or baseline. 
     * 
     * @return ensemble pairs
     */

    public static EnsemblePairs getEnsemblePairsFour()
    {
        //Construct some ensemble pairs
        final TimeWindow window = TimeWindow.of( Instant.parse( FIRST_TIME ),
                                                 Instant.parse( SECOND_TIME ),
                                                 Duration.ofHours( 24 ) );
        final SampleMetadata meta = new SampleMetadataBuilder().setMeasurementUnit( MeasurementUnit.of( MM_DAY ) )
                                                               .setIdentifier( DatasetIdentifier.of( getLocation( DRRC2 ),
                                                                                                     "MAP" ) )
                                                               .setTimeWindow( window )
                                                               .build();
        return EnsemblePairs.of( Collections.emptyList(), Collections.emptyList(), meta, meta );
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
        final List<DichotomousPair> values = new ArrayList<>();
        //Hits
        for ( int i = 0; i < 82; i++ )
        {
            values.add( DichotomousPair.of( true, true ) );
        }
        //False alarms
        for ( int i = 82; i < 120; i++ )
        {
            values.add( DichotomousPair.of( false, true ) );
        }
        //Misses
        for ( int i = 120; i < 143; i++ )
        {
            values.add( DichotomousPair.of( true, false ) );
        }
        for ( int i = 144; i < 366; i++ )
        {
            values.add( DichotomousPair.of( false, false ) );
        }

        final SampleMetadata meta = SampleMetadata.of( MeasurementUnit.of(),
                                                       DatasetIdentifier.of( getLocation( DRRC2 ),
                                                                             "SQIN",
                                                                             "HEFS" ) );
        return DichotomousPairs.ofDichotomousPairs( values, meta ); //Construct the pairs
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
        final List<MulticategoryPair> values = new ArrayList<>();
        //(1,1)
        for ( int i = 0; i < 24; i++ )
        {
            values.add( MulticategoryPair.of( new boolean[] { true, false, false },
                                              new boolean[] { true, false, false } ) );
        }
        //(1,2)
        for ( int i = 24; i < 87; i++ )
        {
            values.add( MulticategoryPair.of( new boolean[] { false, true, false },
                                              new boolean[] { true, false, false } ) );
        }
        //(1,3)
        for ( int i = 87; i < 118; i++ )
        {
            values.add( MulticategoryPair.of( new boolean[] { false, false, true },
                                              new boolean[] { true, false, false } ) );
        }
        //(2,1)
        for ( int i = 118; i < 181; i++ )
        {
            values.add( MulticategoryPair.of( new boolean[] { true, false, false },
                                              new boolean[] { false, true, false } ) );
        }
        //(2,2)
        for ( int i = 181; i < 284; i++ )
        {
            values.add( MulticategoryPair.of( new boolean[] { false, true, false },
                                              new boolean[] { false, true, false } ) );
        }
        //(2,3)
        for ( int i = 284; i < 426; i++ )
        {
            values.add( MulticategoryPair.of( new boolean[] { false, false, true },
                                              new boolean[] { false, true, false } ) );
        }
        //(3,1)
        for ( int i = 426; i < 481; i++ )
        {
            values.add( MulticategoryPair.of( new boolean[] { true, false, false },
                                              new boolean[] { false, false, true } ) );
        }
        //(3,2)
        for ( int i = 481; i < 591; i++ )
        {
            values.add( MulticategoryPair.of( new boolean[] { false, true, false },
                                              new boolean[] { false, false, true } ) );
        }
        //(3,3)
        for ( int i = 591; i < 788; i++ )
        {
            values.add( MulticategoryPair.of( new boolean[] { false, false, true },
                                              new boolean[] { false, false, true } ) );
        }

        final SampleMetadata meta = SampleMetadata.of( MeasurementUnit.of(),
                                                       DatasetIdentifier.of( getLocation( DRRC2 ),
                                                                             "SQIN",
                                                                             "HEFS" ) );
        return MulticategoryPairs.ofMulticategoryPairs( values, meta ); //Construct the pairs
    }

    /**
     * Returns a set of discrete probability pairs without a baseline.
     * 
     * @return discrete probability pairs
     */

    public static DiscreteProbabilityPairs getDiscreteProbabilityPairsOne()
    {
        //Construct some probabilistic pairs, and use the same pairs as a reference for skill (i.e. skill = 0.0)
        final List<DiscreteProbabilityPair> values = new ArrayList<>();
        values.add( DiscreteProbabilityPair.of( 0, 3.0 / 5.0 ) );
        values.add( DiscreteProbabilityPair.of( 0, 1.0 / 5.0 ) );
        values.add( DiscreteProbabilityPair.of( 1, 2.0 / 5.0 ) );
        values.add( DiscreteProbabilityPair.of( 1, 3.0 / 5.0 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.0 / 5.0 ) );
        values.add( DiscreteProbabilityPair.of( 1, 1.0 / 5.0 ) );

        final SampleMetadata meta = SampleMetadata.of( MeasurementUnit.of(),
                                                       DatasetIdentifier.of( getLocation( DRRC2 ),
                                                                             "SQIN",
                                                                             "HEFS" ) );
        return DiscreteProbabilityPairs.of( values, meta );
    }

    /**
     * Returns a set of discrete probability pairs with a baseline.
     * 
     * @return discrete probability pairs
     */

    public static DiscreteProbabilityPairs getDiscreteProbabilityPairsTwo()
    {
        //Construct some probabilistic pairs, and use some different pairs as a reference
        final List<DiscreteProbabilityPair> values = new ArrayList<>();
        values.add( DiscreteProbabilityPair.of( 0, 3.0 / 5.0 ) );
        values.add( DiscreteProbabilityPair.of( 0, 1.0 / 5.0 ) );
        values.add( DiscreteProbabilityPair.of( 1, 2.0 / 5.0 ) );
        values.add( DiscreteProbabilityPair.of( 1, 3.0 / 5.0 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.0 / 5.0 ) );
        values.add( DiscreteProbabilityPair.of( 1, 1.0 / 5.0 ) );
        final List<DiscreteProbabilityPair> baseline = new ArrayList<>();
        baseline.add( DiscreteProbabilityPair.of( 0, 2.0 / 5.0 ) );
        baseline.add( DiscreteProbabilityPair.of( 0, 2.0 / 5.0 ) );
        baseline.add( DiscreteProbabilityPair.of( 1, 1.0 ) );
        baseline.add( DiscreteProbabilityPair.of( 1, 3.0 / 5.0 ) );
        baseline.add( DiscreteProbabilityPair.of( 0, 4.0 / 5.0 ) );
        baseline.add( DiscreteProbabilityPair.of( 1, 1.0 / 5.0 ) );
        final SampleMetadata main = SampleMetadata.of( MeasurementUnit.of(),
                                                       DatasetIdentifier.of( getLocation( DRRC2 ),
                                                                             "SQIN",
                                                                             "HEFS" ) );
        final SampleMetadata base = SampleMetadata.of( MeasurementUnit.of(),
                                                       DatasetIdentifier.of( getLocation( DRRC2 ),
                                                                             "SQIN",
                                                                             "ESP" ) );
        return DiscreteProbabilityPairs.of( values, baseline, main, base );
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
        final List<DiscreteProbabilityPair> values = new ArrayList<>();
        values.add( DiscreteProbabilityPair.of( 0, 0.3 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.1 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.1 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.2 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.2 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.1 ) );
        values.add( DiscreteProbabilityPair.of( 1, 0.4 ) );
        values.add( DiscreteProbabilityPair.of( 1, 0.7 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.7 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.2 ) );
        values.add( DiscreteProbabilityPair.of( 1, 0.2 ) );
        values.add( DiscreteProbabilityPair.of( 1, 1 ) );
        values.add( DiscreteProbabilityPair.of( 1, 0.7 ) );
        values.add( DiscreteProbabilityPair.of( 1, 0.7 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.6 ) );
        values.add( DiscreteProbabilityPair.of( 1, 0.4 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.3 ) );
        values.add( DiscreteProbabilityPair.of( 1, 0.7 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.9 ) );
        values.add( DiscreteProbabilityPair.of( 1, 0.8 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.2 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.2 ) );
        values.add( DiscreteProbabilityPair.of( 1, 0.8 ) );
        values.add( DiscreteProbabilityPair.of( 1, 0.5 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.3 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.1 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.2 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.8 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.5 ) );
        values.add( DiscreteProbabilityPair.of( 1, 0.7 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.8 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.2 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.2 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.3 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.1 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.1 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.1 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.2 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.2 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.2 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.2 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.2 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.1 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.1 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.1 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.1 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.1 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.5 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.2 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.5 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.3 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.3 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.2 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.2 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.1 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.2 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.2 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.3 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.2 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.2 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.1 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0 ) );
        values.add( DiscreteProbabilityPair.of( 1, 0 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.3 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.2 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.2 ) );
        values.add( DiscreteProbabilityPair.of( 1, 0.9 ) );
        values.add( DiscreteProbabilityPair.of( 1, 0.6 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.3 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.3 ) );
        values.add( DiscreteProbabilityPair.of( 1, 0.8 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.5 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.7 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.6 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.3 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.3 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.1 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.1 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.1 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.1 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.2 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.2 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.1 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.2 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.2 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.2 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.2 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.2 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.3 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.7 ) );
        values.add( DiscreteProbabilityPair.of( 1, 1 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.2 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.1 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.2 ) );
        values.add( DiscreteProbabilityPair.of( 1, 0.9 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.6 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.1 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.1 ) );
        values.add( DiscreteProbabilityPair.of( 1, 0.2 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.1 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.8 ) );
        values.add( DiscreteProbabilityPair.of( 1, 1 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.1 ) );
        values.add( DiscreteProbabilityPair.of( 1, 1 ) );
        values.add( DiscreteProbabilityPair.of( 1, 0.8 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.5 ) );
        values.add( DiscreteProbabilityPair.of( 1, 0.6 ) );
        values.add( DiscreteProbabilityPair.of( 1, 0.5 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.2 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.1 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.2 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.7 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.2 ) );
        values.add( DiscreteProbabilityPair.of( 1, 0.8 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.1 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.2 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.6 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.1 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.1 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.2 ) );
        values.add( DiscreteProbabilityPair.of( 1, 0.3 ) );
        values.add( DiscreteProbabilityPair.of( 1, 0.3 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.7 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.6 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.1 ) );
        values.add( DiscreteProbabilityPair.of( 1, 0.9 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.2 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.7 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.7 ) );
        values.add( DiscreteProbabilityPair.of( 1, 0.5 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.7 ) );
        values.add( DiscreteProbabilityPair.of( 1, 0.6 ) );
        values.add( DiscreteProbabilityPair.of( 1, 0.4 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.2 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.8 ) );
        values.add( DiscreteProbabilityPair.of( 1, 0.7 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.4 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.3 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.2 ) );
        values.add( DiscreteProbabilityPair.of( 1, 0.3 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.3 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.7 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.2 ) );
        values.add( DiscreteProbabilityPair.of( 1, 0.7 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.8 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.4 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.2 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.4 ) );
        values.add( DiscreteProbabilityPair.of( 1, 0.7 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.3 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.5 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.5 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.1 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.1 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.7 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.3 ) );
        values.add( DiscreteProbabilityPair.of( 1, 0.2 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.3 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.2 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.2 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.3 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.2 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.5 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.5 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.2 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.3 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.4 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.4 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.4 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.6 ) );
        values.add( DiscreteProbabilityPair.of( 1, 0.8 ) );
        values.add( DiscreteProbabilityPair.of( 1, 0.8 ) );
        values.add( DiscreteProbabilityPair.of( 1, 0.9 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.6 ) );
        values.add( DiscreteProbabilityPair.of( 1, 0.5 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.4 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.4 ) );
        values.add( DiscreteProbabilityPair.of( 1, 0.6 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.7 ) );
        values.add( DiscreteProbabilityPair.of( 1, 0.8 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.3 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.1 ) );
        values.add( DiscreteProbabilityPair.of( 1, 0.5 ) );
        values.add( DiscreteProbabilityPair.of( 1, 0.3 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.7 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.2 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.3 ) );
        values.add( DiscreteProbabilityPair.of( 1, 0.7 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.6 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.4 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.2 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.2 ) );
        values.add( DiscreteProbabilityPair.of( 1, 0.7 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.7 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.5 ) );
        values.add( DiscreteProbabilityPair.of( 1, 0.9 ) );
        values.add( DiscreteProbabilityPair.of( 1, 0.7 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.4 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.6 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.7 ) );
        values.add( DiscreteProbabilityPair.of( 0, 1 ) );
        values.add( DiscreteProbabilityPair.of( 1, 0.8 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.3 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.6 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.5 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.1 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.5 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.3 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.1 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.1 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.2 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.4 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.2 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.1 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.3 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.3 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.6 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.3 ) );
        values.add( DiscreteProbabilityPair.of( 1, 0.9 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.1 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.7 ) );
        values.add( DiscreteProbabilityPair.of( 0, 1 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.9 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.2 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.5 ) );
        values.add( DiscreteProbabilityPair.of( 1, 0.8 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.3 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.6 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.6 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.7 ) );
        values.add( DiscreteProbabilityPair.of( 1, 0.8 ) );
        values.add( DiscreteProbabilityPair.of( 1, 1 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.3 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.2 ) );
        values.add( DiscreteProbabilityPair.of( 1, 1 ) );
        values.add( DiscreteProbabilityPair.of( 1, 0.7 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.4 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.1 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.1 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.3 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.3 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.2 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.1 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.1 ) );
        values.add( DiscreteProbabilityPair.of( 1, 0.8 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.8 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.3 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.1 ) );
        values.add( DiscreteProbabilityPair.of( 1, 0.5 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.4 ) );
        values.add( DiscreteProbabilityPair.of( 1, 0.7 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.3 ) );
        values.add( DiscreteProbabilityPair.of( 1, 0.8 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.7 ) );
        values.add( DiscreteProbabilityPair.of( 1, 0.7 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.1 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.1 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.1 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0 ) );
        values.add( DiscreteProbabilityPair.of( 1, 0.5 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0 ) );
        values.add( DiscreteProbabilityPair.of( 1, 1 ) );
        values.add( DiscreteProbabilityPair.of( 1, 0.7 ) );
        values.add( DiscreteProbabilityPair.of( 1, 0.5 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.6 ) );
        values.add( DiscreteProbabilityPair.of( 1, 0.4 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.3 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.4 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.3 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0 ) );
        values.add( DiscreteProbabilityPair.of( 1, 0.8 ) );
        values.add( DiscreteProbabilityPair.of( 1, 1 ) );
        values.add( DiscreteProbabilityPair.of( 1, 1 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.8 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.5 ) );
        values.add( DiscreteProbabilityPair.of( 1, 0.2 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.1 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.1 ) );
        values.add( DiscreteProbabilityPair.of( 1, 0.2 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.1 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.1 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.9 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.1 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.6 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.7 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.1 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.1 ) );
        values.add( DiscreteProbabilityPair.of( 1, 1 ) );
        values.add( DiscreteProbabilityPair.of( 1, 0.6 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.2 ) );
        values.add( DiscreteProbabilityPair.of( 1, 0.8 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.8 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.6 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.4 ) );
        values.add( DiscreteProbabilityPair.of( 1, 0.6 ) );
        values.add( DiscreteProbabilityPair.of( 1, 0.3 ) );
        values.add( DiscreteProbabilityPair.of( 1, 0.1 ) );
        values.add( DiscreteProbabilityPair.of( 1, 0.9 ) );
        values.add( DiscreteProbabilityPair.of( 1, 0.7 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.3 ) );
        values.add( DiscreteProbabilityPair.of( 1, 0.8 ) );
        values.add( DiscreteProbabilityPair.of( 1, 1 ) );
        values.add( DiscreteProbabilityPair.of( 1, 0.9 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.1 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.1 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.1 ) );

        final SampleMetadata main = SampleMetadata.of( MeasurementUnit.of(),
                                                       DatasetIdentifier.of( getLocation( "Tampere" ),
                                                                             "MAP",
                                                                             "FMI" ) );
        return DiscreteProbabilityPairs.of( values, main );
    }

    /**
     * Returns a set of discrete probability pairs without a baseline and comprising observed non-occurrences only.
     * 
     * @return discrete probability pairs with observed non-occurrences
     */

    public static DiscreteProbabilityPairs getDiscreteProbabilityPairsFour()
    {
        final List<DiscreteProbabilityPair> values = new ArrayList<>();
        values.add( DiscreteProbabilityPair.of( 0, 3.0 / 5.0 ) );
        values.add( DiscreteProbabilityPair.of( 0, 1.0 / 5.0 ) );
        values.add( DiscreteProbabilityPair.of( 0, 2.0 / 5.0 ) );
        values.add( DiscreteProbabilityPair.of( 0, 3.0 / 5.0 ) );
        values.add( DiscreteProbabilityPair.of( 0, 0.0 / 5.0 ) );
        values.add( DiscreteProbabilityPair.of( 0, 1.0 / 5.0 ) );

        final SampleMetadata meta = SampleMetadata.of( MeasurementUnit.of(),
                                                       DatasetIdentifier.of( getLocation( DRRC2 ),
                                                                             "SQIN",
                                                                             "HEFS" ) );
        return DiscreteProbabilityPairs.of( values, meta );
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
                new TimeSeriesOfSingleValuedPairsBuilder();
        // Create a regular time-series with an issue date/time, a series of paired values, and a timestep
        Instant firstId = Instant.parse( FIRST_TIME );
        List<Event<SingleValuedPair>> firstValues = new ArrayList<>();
        // Add some values
        firstValues.add( Event.of( firstId, Instant.parse( "1985-01-01T06:00:00Z" ), SingleValuedPair.of( 1, 1 ) ) );
        firstValues.add( Event.of( firstId, Instant.parse( "1985-01-01T12:00:00Z" ), SingleValuedPair.of( 1, 5 ) ) );
        firstValues.add( Event.of( firstId, Instant.parse( "1985-01-01T18:00:00Z" ), SingleValuedPair.of( 5, 1 ) ) );

        // Add another time-series
        Instant secondId = Instant.parse( THIRD_TIME );
        List<Event<SingleValuedPair>> secondValues = new ArrayList<>();
        // Add some values
        secondValues.add( Event.of( secondId, Instant.parse( FOURTH_TIME ), SingleValuedPair.of( 10, 1 ) ) );
        secondValues.add( Event.of( secondId, Instant.parse( FIFTH_TIME ), SingleValuedPair.of( 1, 1 ) ) );
        secondValues.add( Event.of( secondId, Instant.parse( SIXTH_TIME ), SingleValuedPair.of( 1, 10 ) ) );

        // Create some default metadata for the time-series
        final TimeWindow window = TimeWindow.of( Instant.parse( FIRST_TIME ),
                                                 Instant.parse( THIRD_TIME ),
                                                 Duration.ofHours( 6 ),
                                                 Duration.ofHours( 18 ) );
        final SampleMetadata metaData = new SampleMetadataBuilder().setMeasurementUnit( MeasurementUnit.of( "CMS" ) )
                                                                   .setIdentifier( DatasetIdentifier.of( getLocation( "A" ),
                                                                                                         STREAMFLOW ) )
                                                                   .setTimeWindow( window )
                                                                   .build();
        // Build the time-series
        return (TimeSeriesOfSingleValuedPairs) builder.addTimeSeries( firstValues )
                                                      .addTimeSeries( secondValues )
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
                new TimeSeriesOfSingleValuedPairsBuilder();
        // Create a regular time-series with an issue date/time, a series of paired values, and a timestep
        Instant firstId = Instant.parse( FIRST_TIME );
        List<Event<SingleValuedPair>> firstValues = new ArrayList<>();
        // Add some values
        firstValues.add( Event.of( firstId, Instant.parse( "1985-01-01T06:00:00Z" ), SingleValuedPair.of( 1, 1 ) ) );
        firstValues.add( Event.of( firstId, Instant.parse( "1985-01-01T12:00:00Z" ), SingleValuedPair.of( 1, 5 ) ) );
        firstValues.add( Event.of( firstId, Instant.parse( "1985-01-01T18:00:00Z" ), SingleValuedPair.of( 5, 1 ) ) );

        // Create some default metadata for the time-series
        final TimeWindow window = TimeWindow.of( Instant.parse( FIRST_TIME ),
                                                 Instant.parse( FIRST_TIME ),
                                                 Duration.ofHours( 6 ),
                                                 Duration.ofHours( 18 ) );
        final SampleMetadata metaData = new SampleMetadataBuilder().setMeasurementUnit( MeasurementUnit.of( "CMS" ) )
                                                                   .setIdentifier( DatasetIdentifier.of( getLocation( "A" ),
                                                                                                         STREAMFLOW ) )
                                                                   .setTimeWindow( window )
                                                                   .build();
        // Build the time-series
        return (TimeSeriesOfSingleValuedPairs) builder.addTimeSeries( firstValues )
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
                new TimeSeriesOfSingleValuedPairsBuilder();
        // Create a regular time-series with an issue date/time, a series of paired values, and a timestep

        // Add another time-series
        Instant secondId = Instant.parse( THIRD_TIME );
        List<Event<SingleValuedPair>> secondValues = new ArrayList<>();
        // Add some values
        secondValues.add( Event.of( secondId, Instant.parse( FOURTH_TIME ), SingleValuedPair.of( 10, 1 ) ) );
        secondValues.add( Event.of( secondId, Instant.parse( FIFTH_TIME ), SingleValuedPair.of( 1, 1 ) ) );
        secondValues.add( Event.of( secondId, Instant.parse( SIXTH_TIME ), SingleValuedPair.of( 1, 10 ) ) );

        // Create some default metadata for the time-series
        final TimeWindow window = TimeWindow.of( Instant.parse( THIRD_TIME ),
                                                 Instant.parse( THIRD_TIME ),
                                                 Duration.ofHours( 6 ),
                                                 Duration.ofHours( 18 ) );
        final SampleMetadata metaData = new SampleMetadataBuilder().setMeasurementUnit( MeasurementUnit.of( "CMS" ) )
                                                                   .setIdentifier( DatasetIdentifier.of( getLocation( "A" ),
                                                                                                         STREAMFLOW ) )
                                                                   .setTimeWindow( window )
                                                                   .build();
        // Build the time-series
        return (TimeSeriesOfSingleValuedPairs) builder.addTimeSeries( secondValues )
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
                new TimeSeriesOfSingleValuedPairsBuilder();
        // Create a regular time-series with an issue date/time, a series of paired values, and a timestep

        // Create some default metadata for the time-series
        final TimeWindow window = TimeWindow.of( Instant.MIN,
                                                 Instant.MAX );
        final SampleMetadata metaData = new SampleMetadataBuilder().setMeasurementUnit( MeasurementUnit.of( "CMS" ) )
                                                                   .setIdentifier( DatasetIdentifier.of( getLocation( "A" ),
                                                                                                         STREAMFLOW ) )
                                                                   .setTimeWindow( window )
                                                                   .build();
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
                new TimeSeriesOfSingleValuedPairsBuilder();
        // Create a regular time-series with an issue date/time, a series of paired values, and a timestep

        // Add another time-series
        Instant secondId = Instant.parse( THIRD_TIME );
        List<Event<SingleValuedPair>> secondValues = new ArrayList<>();

        // Add some values
        secondValues.add( Event.of( secondId, Instant.parse( FOURTH_TIME ), SingleValuedPair.of( 10, 1 ) ) );
        secondValues.add( Event.of( secondId, Instant.parse( FIFTH_TIME ), SingleValuedPair.of( 1, 1 ) ) );
        secondValues.add( Event.of( secondId,
                                    Instant.parse( SIXTH_TIME ),
                                    SingleValuedPair.of( 10, 10 ) ) );
        secondValues.add( Event.of( secondId, Instant.parse( "1985-01-03T00:00:00Z" ), SingleValuedPair.of( 2, 10 ) ) );
        secondValues.add( Event.of( secondId, Instant.parse( "1985-01-03T06:00:00Z" ), SingleValuedPair.of( 4, 7 ) ) );

        // Create some default metadata for the time-series
        final TimeWindow window = TimeWindow.of( Instant.parse( THIRD_TIME ),
                                                 Instant.parse( THIRD_TIME ),
                                                 Duration.ofHours( 6 ),
                                                 Duration.ofHours( 30 ) );
        final SampleMetadata metaData = new SampleMetadataBuilder().setMeasurementUnit( MeasurementUnit.of( "CMS" ) )
                                                                   .setIdentifier( DatasetIdentifier.of( getLocation( "A" ),
                                                                                                         STREAMFLOW ) )
                                                                   .setTimeWindow( window )
                                                                   .build();
        // Build the time-series
        return (TimeSeriesOfSingleValuedPairs) builder.addTimeSeries( secondValues )
                                                      .setMetadata( metaData )
                                                      .build();
    }

    /**
     * Hidden constructor
     */
    private MetricTestDataFactory()
    {
    }

}
