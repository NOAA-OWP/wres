package wres.datamodel;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.StringJoiner;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import wres.datamodel.SafeRegularTimeSeriesOfEnsemblePairs.SafeRegularTimeSeriesOfEnsemblePairsBuilder;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.PairOfDoubleAndVectorOfDoubles;
import wres.datamodel.inputs.pairs.PairOfDoubles;
import wres.datamodel.inputs.pairs.TimeSeriesOfEnsemblePairs;
import wres.datamodel.metadata.Metadata;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.time.TimeSeries;

/**
 * Tests the {@link SafeRegularTimeSeriesOfEnsemblePairs}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.3
 */
public final class SafeRegularTimeSeriesOfEnsemblePairsTest
{


    /**
     * Tests the {@link SafeRegularTimeSeriesOfEnsemblePairs#basisTimeIterator()} method.
     */

    @Test
    public void test1BasisTimeIterator()
    {
        //Build a time-series with three basis times 
        List<PairOfDoubleAndVectorOfDoubles> first = new ArrayList<>();
        List<PairOfDoubleAndVectorOfDoubles> second = new ArrayList<>();
        List<PairOfDoubleAndVectorOfDoubles> third = new ArrayList<>();
        SafeRegularTimeSeriesOfEnsemblePairsBuilder b = new SafeRegularTimeSeriesOfEnsemblePairsBuilder();
        DataFactory metIn = DefaultDataFactory.getInstance();
        MetadataFactory metaFac = metIn.getMetadataFactory();
        Instant firstBasisTime = Instant.parse( "1985-01-01T00:00:00Z" );
        first.add( metIn.pairOf( 1, new double[] { 1, 2, 3 } ) );
        first.add( metIn.pairOf( 2, new double[] { 2, 3, 4 } ) );
        first.add( metIn.pairOf( 3, new double[] { 3, 4, 5 } ) );
        Instant secondBasisTime = Instant.parse( "1985-01-02T00:00:00Z" );
        second.add( metIn.pairOf( 4, new double[] { 4, 5, 6 } ) );
        second.add( metIn.pairOf( 5, new double[] { 5, 6, 7 } ) );
        second.add( metIn.pairOf( 6, new double[] { 6, 7, 8 } ) );
        Instant thirdBasisTime = Instant.parse( "1985-01-03T00:00:00Z" );
        third.add( metIn.pairOf( 7, new double[] { 7, 8, 9 } ) );
        third.add( metIn.pairOf( 8, new double[] { 8, 9, 10 } ) );
        third.add( metIn.pairOf( 9, new double[] { 9, 10, 11 } ) );
        final Metadata meta = metaFac.getMetadata();
        TimeSeriesOfEnsemblePairs ts =
                (TimeSeriesOfEnsemblePairs) b.addData( firstBasisTime, first )
                                             .addData( secondBasisTime, second )
                                             .addData( thirdBasisTime, third )
                                             .setTimeStep( Duration.ofDays( 1 ) )
                                             .setMetadata( meta )
                                             .build();

        assertTrue( "Expected a time-series container with multiple basis times.", ts.hasMultipleTimeSeries() );

        //Iterate and test
        int nextValue = 1;
        for ( TimeSeries<PairOfDoubleAndVectorOfDoubles> next : ts.basisTimeIterator() )
        {
            for ( Pair<Instant, PairOfDoubleAndVectorOfDoubles> nextPair : next.timeIterator() )
            {
                assertTrue( "Unexpected pair in basis-time iteration of time-series.",
                            nextPair.getRight()
                                    .equals( metIn.pairOf( nextValue,
                                                           new double[] { nextValue, nextValue + 1,
                                                                          nextValue + 2 } ) ) );
                nextValue++;
            }
        }
    }

    /**
     * Tests the {@link SafeRegularTimeSeriesOfEnsemblePairs#durationIterator()} method.
     */

    @Test
    public void test2DurationIterator()
    {
        //Build a time-series with three basis times 
        List<PairOfDoubleAndVectorOfDoubles> first = new ArrayList<>();
        List<PairOfDoubleAndVectorOfDoubles> second = new ArrayList<>();
        List<PairOfDoubleAndVectorOfDoubles> third = new ArrayList<>();
        SafeRegularTimeSeriesOfEnsemblePairsBuilder b = new SafeRegularTimeSeriesOfEnsemblePairsBuilder();
        DataFactory metIn = DefaultDataFactory.getInstance();
        MetadataFactory metaFac = metIn.getMetadataFactory();
        Instant firstBasisTime = Instant.parse( "1985-01-01T00:00:00Z" );
        first.add( metIn.pairOf( 1, new double[] { 1, 2, 3 } ) );
        first.add( metIn.pairOf( 2, new double[] { 2, 3, 4 } ) );
        first.add( metIn.pairOf( 3, new double[] { 3, 4, 5 } ) );
        Instant secondBasisTime = Instant.parse( "1985-01-02T00:00:00Z" );
        second.add( metIn.pairOf( 1, new double[] { 1, 2, 3 } ) );
        second.add( metIn.pairOf( 2, new double[] { 2, 3, 4 } ) );
        second.add( metIn.pairOf( 3, new double[] { 3, 4, 5 } ) );
        Instant thirdBasisTime = Instant.parse( "1985-01-03T00:00:00Z" );
        third.add( metIn.pairOf( 1, new double[] { 1, 2, 3 } ) );
        third.add( metIn.pairOf( 2, new double[] { 2, 3, 4 } ) );
        third.add( metIn.pairOf( 3, new double[] { 3, 4, 5 } ) );
        Metadata meta = metaFac.getMetadata();
        //Add the time-series, with only one for baseline
        TimeSeriesOfEnsemblePairs ts =
                (TimeSeriesOfEnsemblePairs) b.addData( firstBasisTime, first )
                                             .addData( secondBasisTime, second )
                                             .addData( thirdBasisTime, third )
                                             .addDataForBaseline( firstBasisTime, first )
                                             .setTimeStep( Duration.ofDays( 1 ) )
                                             .setMetadata( meta )
                                             .setMetadataForBaseline( meta )
                                             .build();

        assertTrue( "Expected a regular time-series for iteration.", ts.isRegular() );

        //Iterate and test
        int nextValue = 1;
        for ( TimeSeries<PairOfDoubleAndVectorOfDoubles> next : ts.durationIterator() )
        {
            for ( Pair<Instant, PairOfDoubleAndVectorOfDoubles> nextPair : next.timeIterator() )
            {
                assertTrue( "Unexpected pair in lead-time iteration of time-series.",
                            nextPair.getRight()
                                    .equals( metIn.pairOf( nextValue,
                                                           new double[] { nextValue, nextValue + 1,
                                                                          nextValue + 2 } ) ) );
            }
            //Three time-series for main, one for baseline.
            assertTrue( "Unexpected number of time-series in dataset.",
                        next.getBasisTimes().size() == 3 );
            assertTrue( "Unexpected number of time-series in baseline dataset.",
                        ( (TimeSeriesOfEnsemblePairs) next ).getDataForBaseline().size() == 1 );
            nextValue++;
        }
    }

    /**
     * Tests the {@link SafeRegularTimeSeriesOfEnsemblePairs#ensembleTraceIterator()} method.
     */

    @Test
    public void test3EnsembleTraceIterator()
    {
        //Build a time-series with three basis times 
        List<PairOfDoubleAndVectorOfDoubles> first = new ArrayList<>();
        List<PairOfDoubleAndVectorOfDoubles> second = new ArrayList<>();
        List<PairOfDoubleAndVectorOfDoubles> third = new ArrayList<>();
        SafeRegularTimeSeriesOfEnsemblePairsBuilder b = new SafeRegularTimeSeriesOfEnsemblePairsBuilder();
        DataFactory metIn = DefaultDataFactory.getInstance();
        MetadataFactory metaFac = metIn.getMetadataFactory();
        Instant firstBasisTime = Instant.parse( "1985-01-01T00:00:00Z" );
        first.add( metIn.pairOf( 1, new double[] { 1, 2, 3 } ) );
        first.add( metIn.pairOf( 1, new double[] { 1, 2, 3 } ) );
        first.add( metIn.pairOf( 1, new double[] { 1, 2, 3 } ) );
        Instant secondBasisTime = Instant.parse( "1985-01-02T00:00:00Z" );
        second.add( metIn.pairOf( 1, new double[] { 4, 5, 6 } ) );
        second.add( metIn.pairOf( 1, new double[] { 4, 5, 6 } ) );
        second.add( metIn.pairOf( 1, new double[] { 4, 5, 6 } ) );
        Instant thirdBasisTime = Instant.parse( "1985-01-03T00:00:00Z" );
        third.add( metIn.pairOf( 1, new double[] { 7, 8, 9 } ) );
        third.add( metIn.pairOf( 1, new double[] { 7, 8, 9 } ) );
        third.add( metIn.pairOf( 1, new double[] { 7, 8, 9 } ) );
        Metadata meta = metaFac.getMetadata();
        //Add the time-series, with only one for baseline
        TimeSeriesOfEnsemblePairs ts =
                (TimeSeriesOfEnsemblePairs) b.addData( firstBasisTime, first )
                                             .addData( secondBasisTime, second )
                                             .addData( thirdBasisTime, third )
                                             .addDataForBaseline( firstBasisTime, first )
                                             .setTimeStep( Duration.ofDays( 1 ) )
                                             .setMetadata( meta )
                                             .setMetadataForBaseline( meta )
                                             .build();

        assertTrue( "Expected a regular time-series for iteration.", ts.isRegular() );

        //Iterate and test
        int nextValue = 1;
        for ( TimeSeries<PairOfDoubles> next : ts.ensembleTraceIterator() )
        {
            for ( Pair<Instant, PairOfDoubles> nextPair : next.timeIterator() )
            {
                assertTrue( "Unexpected pair in ensemble trace iteration of time-series.",
                            nextPair.getRight()
                                    .equals( metIn.pairOf( 1, nextValue ) ) );
            }
            //Three time-series for main, one for baseline.
            assertTrue( "Unexpected number of time-series in dataset.",
                        next.getBasisTimes().size() == 1 );
            nextValue++;
        }
    }

    /**
     * Tests the {@link SafeRegularTimeSeriesOfEnsemblePairs#getBaselineData()} method.
     */

    @Test
    public void test4GetBaselineData()
    {
        //Build a time-series with two basis times
        List<PairOfDoubleAndVectorOfDoubles> values = new ArrayList<>();
        SafeRegularTimeSeriesOfEnsemblePairsBuilder b = new SafeRegularTimeSeriesOfEnsemblePairsBuilder();
        DataFactory metIn = DefaultDataFactory.getInstance();
        MetadataFactory metaFac = metIn.getMetadataFactory();
        Instant basisTime = Instant.parse( "1985-01-01T00:00:00Z" );
        values.add( metIn.pairOf( 1, new double[] { 1, 2, 3 } ) );
        values.add( metIn.pairOf( 2, new double[] { 2, 3, 4 } ) );
        values.add( metIn.pairOf( 3, new double[] { 3, 4, 5 } ) );
        Metadata meta = metaFac.getMetadata();
        b.addData( basisTime, values ).setTimeStep( Duration.ofDays( 1 ) ).setMetadata( meta );
        //Check dataset dimensions
        assertTrue( "Unexpected baseline associated with time-series.",
                    Objects.isNull( b.build().getBaselineData() ) );

        b.addDataForBaseline( basisTime, values );
        b.setMetadataForBaseline( meta );

        TimeSeriesOfEnsemblePairs baseline = b.build().getBaselineData();

        //Check dataset dimensions
        assertTrue( "Expected a time-series with one basis time and three lead times.",
                    baseline.getDurations().size() == 3 && baseline.getBasisTimes().size() == 1 );

        //Check dataset
        //Iterate and test
        int nextValue = 1;
        for ( TimeSeries<PairOfDoubleAndVectorOfDoubles> next : baseline.durationIterator() )
        {
            for ( Pair<Instant, PairOfDoubleAndVectorOfDoubles> nextPair : next.timeIterator() )
            {
                assertTrue( "Unexpected pair in lead-time iteration of baseline time-series.",
                            nextPair.getRight()
                                    .equals( metIn.pairOf( nextValue,
                                                           new double[] { nextValue, nextValue + 1,
                                                                          nextValue + 2 } ) ) );
                nextValue++;
            }
        }
    }

    /**
     * Tests the addition of several time-series with a common basis time.
     */

    @Test
    public void test5AddMultipleTimeSeriesWithSameBasisTime()
    {
        //Build a time-series with one basis times and three separate sets of data to append
        List<PairOfDoubleAndVectorOfDoubles> first = new ArrayList<>();
        List<PairOfDoubleAndVectorOfDoubles> second = new ArrayList<>();
        List<PairOfDoubleAndVectorOfDoubles> third = new ArrayList<>();

        SafeRegularTimeSeriesOfEnsemblePairsBuilder b = new SafeRegularTimeSeriesOfEnsemblePairsBuilder();
        DataFactory metIn = DefaultDataFactory.getInstance();
        MetadataFactory metaFac = metIn.getMetadataFactory();
        Instant basisTime = Instant.parse( "1985-01-01T00:00:00Z" );
        first.add( metIn.pairOf( 1, new double[] { 1, 2, 3 } ) );
        first.add( metIn.pairOf( 2, new double[] { 2, 3, 4 } ) );
        first.add( metIn.pairOf( 3, new double[] { 3, 4, 5 } ) );
        Metadata meta = metaFac.getMetadata();
        b.addData( basisTime, first )
         .addDataForBaseline( basisTime, first )
         .setTimeStep( Duration.ofDays( 1 ) )
         .setMetadata( meta )
         .setMetadataForBaseline( meta );

        //Build the first ts
        TimeSeriesOfEnsemblePairs ts = b.build();
        //Add the first time-series and then append a second and third
        SafeRegularTimeSeriesOfEnsemblePairsBuilder c = new SafeRegularTimeSeriesOfEnsemblePairsBuilder();
        c.addTimeSeries( ts );
        second.add( metIn.pairOf( 4, new double[] { 4, 5, 6 } ) );
        second.add( metIn.pairOf( 5, new double[] { 5, 6, 7 } ) );
        second.add( metIn.pairOf( 6, new double[] { 6, 7, 8 } ) );
        third.add( metIn.pairOf( 7, new double[] { 7, 8, 9 } ) );
        third.add( metIn.pairOf( 8, new double[] { 8, 9, 10 } ) );
        third.add( metIn.pairOf( 9, new double[] { 9, 10, 11 } ) );
        c.addData( basisTime, second )
         .addData( basisTime, third )
         .addDataForBaseline( basisTime, second )
         .addDataForBaseline( basisTime, third );
        
        TimeSeriesOfEnsemblePairs tsCombined = c.build();
        
        //Check dataset dimensions
        assertTrue( "Expected a time-series with three basis times and three lead times.",
                    tsCombined.getDurations().size() == 3 && tsCombined.getBasisTimes().size() == 3 );
        //Check dataset
        //Iterate and test
        int nextValue = 1;
        for ( Pair<Instant, PairOfDoubleAndVectorOfDoubles> nextPair : tsCombined.timeIterator() )
        {
            assertTrue( "Unexpected pair in lead-time iteration of baseline time-series.",
                        nextPair.getRight()
                                .equals( metIn.pairOf( nextValue,
                                                       new double[] { nextValue, nextValue + 1, nextValue + 2 } ) ) );
            nextValue++;
        }
    }

    /**
     * Tests the {@link SafeRegularTimeSeriesOfEnsemblePairs#filterByBasisTime(java.util.function.Predicate)} 
     * method.
     */

    @Test
    public void test6FilterByBasisTime()
    {
        //Build a time-series with three basis times 
        List<PairOfDoubleAndVectorOfDoubles> first = new ArrayList<>();
        List<PairOfDoubleAndVectorOfDoubles> second = new ArrayList<>();
        List<PairOfDoubleAndVectorOfDoubles> third = new ArrayList<>();
        SafeRegularTimeSeriesOfEnsemblePairsBuilder b = new SafeRegularTimeSeriesOfEnsemblePairsBuilder();
        DataFactory metIn = DefaultDataFactory.getInstance();
        MetadataFactory metaFac = metIn.getMetadataFactory();
        Instant firstBasisTime = Instant.parse( "1985-01-01T00:00:00Z" );
        first.add( metIn.pairOf( 1, new double[] { 1, 2, 3 } ) );
        first.add( metIn.pairOf( 2, new double[] { 2, 3, 4 } ) );
        first.add( metIn.pairOf( 3, new double[] { 3, 4, 5 } ) );
        Instant secondBasisTime = Instant.parse( "1985-01-02T00:00:00Z" );
        second.add( metIn.pairOf( 4, new double[] { 4, 5, 6 } ) );
        second.add( metIn.pairOf( 5, new double[] { 5, 6, 7 } ) );
        second.add( metIn.pairOf( 6, new double[] { 6, 7, 8 } ) );
        Instant thirdBasisTime = Instant.parse( "1985-01-03T00:00:00Z" );
        third.add( metIn.pairOf( 7, new double[] { 7, 8, 9 } ) );
        third.add( metIn.pairOf( 8, new double[] { 8, 9, 10 } ) );
        third.add( metIn.pairOf( 9, new double[] { 9, 10, 11 } ) );
        Metadata meta = metaFac.getMetadata();
        //Add the time-series
        TimeSeriesOfEnsemblePairs ts =
                (TimeSeriesOfEnsemblePairs) b.addData( firstBasisTime, first )
                                             .addData( secondBasisTime, second )
                                             .addData( thirdBasisTime, third )
                                             .setTimeStep( Duration.ofDays( 1 ) )
                                             .setMetadata( meta )
                                             .build();
        //Iterate and test
        TimeSeries<PairOfDoubleAndVectorOfDoubles> filtered = ts.filterByBasisTime( a -> a.equals( secondBasisTime ) );
        assertTrue( "Unexpected number of issue times in the filtered time-series.",
                    filtered.getBasisTimes().size() == 1 );
        assertTrue( "Unexpected issue time in the filtered time-series.",
                    filtered.getBasisTimes().get( 0 ).equals( secondBasisTime ) );
        assertTrue( "Unexpected value in the filtered time-series.",
                    filtered.timeIterator()
                            .iterator()
                            .next()
                            .getRight()
                            .equals( metIn.pairOf( 4, new double[] { 4, 5, 6 } ) ) );
        //Check for nullity on none filter
        assertTrue( "Expected nullity on filtering basis times.",
                    Objects.isNull( ts.filterByBasisTime( a -> a.equals( Instant.parse( "1985-01-04T00:00:00Z" ) ) ) ) );

    }

    /**
     * Tests the {@link SafeRegularTimeSeriesOfEnsemblePairs#filterByTraceIndex(java.util.function.Predicate)} 
     * method.
     */

    @Test
    public void test7FilterByTraceIndex()
    {
        //Build a time-series with three basis times 
        List<PairOfDoubleAndVectorOfDoubles> first = new ArrayList<>();
        List<PairOfDoubleAndVectorOfDoubles> second = new ArrayList<>();
        List<PairOfDoubleAndVectorOfDoubles> third = new ArrayList<>();
        SafeRegularTimeSeriesOfEnsemblePairsBuilder b = new SafeRegularTimeSeriesOfEnsemblePairsBuilder();
        DataFactory metIn = DefaultDataFactory.getInstance();
        MetadataFactory metaFac = metIn.getMetadataFactory();
        Instant firstBasisTime = Instant.parse( "1985-01-01T00:00:00Z" );
        first.add( metIn.pairOf( 1, new double[] { 1, 2, 3, 4, 5 } ) );
        first.add( metIn.pairOf( 2, new double[] { 1, 2, 3, 4, 5 } ) );
        first.add( metIn.pairOf( 3, new double[] { 1, 2, 3, 4, 5 } ) );
        Instant secondBasisTime = Instant.parse( "1985-01-02T00:00:00Z" );
        second.add( metIn.pairOf( 4, new double[] { 6, 7, 8, 9, 10 } ) );
        second.add( metIn.pairOf( 5, new double[] { 6, 7, 8, 9, 10 } ) );
        second.add( metIn.pairOf( 6, new double[] { 6, 7, 8, 9, 10 } ) );
        Instant thirdBasisTime = Instant.parse( "1985-01-03T00:00:00Z" );
        third.add( metIn.pairOf( 7, new double[] { 11, 12, 13, 14, 15 } ) );
        third.add( metIn.pairOf( 8, new double[] { 11, 12, 13, 14, 15 } ) );
        third.add( metIn.pairOf( 9, new double[] { 11, 12, 13, 14, 15 } ) );
        Metadata meta = metaFac.getMetadata();
        //Add the time-series
        TimeSeriesOfEnsemblePairs ts =
                (TimeSeriesOfEnsemblePairs) b.addData( firstBasisTime, first )
                                             .addData( secondBasisTime, second )
                                             .addData( thirdBasisTime, third )
                                             .setTimeStep( Duration.ofDays( 1 ) )
                                             .setMetadata( meta )
                                             .build();
        //Iterate and test
        TimeSeries<PairOfDoubleAndVectorOfDoubles> filtered =
                ts.filterByBasisTime( p -> p.equals( secondBasisTime ) )
                  .filterByDuration( q -> q.equals( Duration.ofDays( 3 ) ) );

        TimeSeriesOfEnsemblePairs regular =
                ( (TimeSeriesOfEnsemblePairs) filtered ).filterByTraceIndex( q -> q.equals( 0 )
                                                                                  || q.equals( 3 ) );

        assertTrue( "Unexpected number of durations in filtered time-series.", regular.getDurations().size() == 1 );
        assertTrue( "Unexpected duration in the filtered time-series.",
                    regular.getDurations().first().equals( Duration.ofDays( 3 ) ) );
        assertTrue( "Unexpected value in the filtered time-series.",
                    regular.timeIterator()
                           .iterator()
                           .next()
                           .getRight()
                           .equals( metIn.pairOf( 6, new double[] { 6, 9 } ) ) );
        //Check for nullity on none filter
        assertTrue( "Expected nullity on filtering durations.",
                    Objects.isNull( ( (TimeSeriesOfEnsemblePairs) filtered ).filterByTraceIndex( q -> q.equals( 10 ) ) ) );
    }

    /**
     * Tests the {@link SafeRegularTimeSeriesOfEnsemblePairs#filterByDuration(java.util.function.Predicate)} 
     * method.
     */

    @Test
    public void test8FilterByDuration()
    {
        //Build a time-series with three basis times 
        List<PairOfDoubleAndVectorOfDoubles> first = new ArrayList<>();
        List<PairOfDoubleAndVectorOfDoubles> second = new ArrayList<>();
        List<PairOfDoubleAndVectorOfDoubles> third = new ArrayList<>();
        SafeRegularTimeSeriesOfEnsemblePairsBuilder b = new SafeRegularTimeSeriesOfEnsemblePairsBuilder();
        DataFactory metIn = DefaultDataFactory.getInstance();
        MetadataFactory metaFac = metIn.getMetadataFactory();
        Instant firstBasisTime = Instant.parse( "1985-01-01T00:00:00Z" );
        first.add( metIn.pairOf( 1, new double[] { 1, 2, 3, } ) );
        first.add( metIn.pairOf( 2, new double[] { 2, 3, 4 } ) );
        first.add( metIn.pairOf( 3, new double[] { 3, 4, 5 } ) );
        Instant secondBasisTime = Instant.parse( "1985-01-02T00:00:00Z" );
        second.add( metIn.pairOf( 4, new double[] { 4, 5, 6 } ) );
        second.add( metIn.pairOf( 5, new double[] { 5, 6, 7 } ) );
        second.add( metIn.pairOf( 6, new double[] { 6, 7, 8 } ) );
        Instant thirdBasisTime = Instant.parse( "1985-01-03T00:00:00Z" );
        third.add( metIn.pairOf( 7, new double[] { 7, 8, 9 } ) );
        third.add( metIn.pairOf( 8, new double[] { 8, 9, 10 } ) );
        third.add( metIn.pairOf( 9, new double[] { 9, 10, 11 } ) );
        Metadata meta = metaFac.getMetadata();
        //Add the time-series
        TimeSeriesOfEnsemblePairs ts =
                (TimeSeriesOfEnsemblePairs) b.addData( firstBasisTime, first )
                                             .addData( secondBasisTime, second )
                                             .addData( thirdBasisTime, third )
                                             .setTimeStep( Duration.ofDays( 1 ) )
                                             .setMetadata( meta )
                                             .build();
        //Iterate and test
        TimeSeries<PairOfDoubleAndVectorOfDoubles> filtered =
                ts.filterByBasisTime( p -> p.equals( secondBasisTime ) )
                  .filterByDuration( q -> q.equals( Duration.ofDays( 3 ) ) );
        assertTrue( "Unexpected number of lead times in filtered time-series.", filtered.getDurations().size() == 1 );
        assertTrue( "Unexpected lead time in the filtered time-series.",
                    filtered.getDurations().first().equals( Duration.ofDays( 3 ) ) );
        assertTrue( "Unexpected value in the filtered time-series.",
                    filtered.timeIterator()
                            .iterator()
                            .next()
                            .getRight()
                            .equals( metIn.pairOf( 6, new double[] { 6, 7, 8 } ) ) );
        //Check for nullity on none filter
        assertTrue( "Expected nullity on filtering lead times.",
                    Objects.isNull( ts.filterByDuration( p -> p.equals( Duration.ofDays( 4 ) ) ) ) );

    }

    /**
     * Tests for exceptional cases.
     */

    @Test
    public void test9Exceptions()
    {
        List<PairOfDoubleAndVectorOfDoubles> first = new ArrayList<>();
        List<PairOfDoubleAndVectorOfDoubles> second = new ArrayList<>();
        SafeRegularTimeSeriesOfEnsemblePairsBuilder b = new SafeRegularTimeSeriesOfEnsemblePairsBuilder();
        SafeRegularTimeSeriesOfEnsemblePairsBuilder c = new SafeRegularTimeSeriesOfEnsemblePairsBuilder();
        DataFactory metIn = DefaultDataFactory.getInstance();
        MetadataFactory metaFac = metIn.getMetadataFactory();
        Instant firstBasisTime = Instant.parse( "1985-01-01T00:00:00Z" );
        first.add( metIn.pairOf( 1, new double[] { 1 } ) );
        first.add( metIn.pairOf( 2, new double[] { 2 } ) );
        first.add( metIn.pairOf( 3, new double[] { 3 } ) );
        Instant secondBasisTime = Instant.parse( "1985-01-02T00:00:00Z" );
        second.add( metIn.pairOf( 4, new double[] { 4 } ) );
        second.add( metIn.pairOf( 5, new double[] { 5 } ) );
        second.add( metIn.pairOf( 6, new double[] { 6 } ) );

        final Metadata meta = metaFac.getMetadata();
        c.addData( firstBasisTime, first ).setTimeStep( Duration.ofDays( 2 ) ).setMetadata( meta );
        try
        {
            b.addData( firstBasisTime, first )
             .addData( secondBasisTime, second )
             .setTimeStep( Duration.ofDays( 1 ) )
             .addTimeSeries( c.build() )
             .setMetadata( meta )
             .build();
            fail( "Expected a checked exception on building a regular time-series with inconsistent durations." );
        }
        catch ( MetricInputException e )
        {
        }

        //Check for exception on construction with main pairs that have a varying number of members
        SafeRegularTimeSeriesOfEnsemblePairsBuilder ca = new SafeRegularTimeSeriesOfEnsemblePairsBuilder();
        List<PairOfDoubleAndVectorOfDoubles> third = new ArrayList<>();
        third.add( metIn.pairOf( 4, new double[] { 4 } ) );
        third.add( metIn.pairOf( 5, new double[] { 5, 6 } ) );
        third.add( metIn.pairOf( 6, new double[] { 7 } ) );
        try
        {
            ca.addData( firstBasisTime, third ).setTimeStep( Duration.ofHours( 1 ) ).setMetadata( meta ).build();
            fail( "Expected a checked exception on building a time-series with a varying number of ensemble members." );
        }
        catch ( MetricInputException e )
        {
            assertTrue( "Unexpected error message on checking member count.",
                        e.getMessage()
                         .equals( "While building a regular time-series of ensemble pairs: each pair must contain "
                                  + "the same number of ensemble members." ) );
        }

        //Check for exception on construction with baseline pairs that have a varying number of members
        SafeRegularTimeSeriesOfEnsemblePairsBuilder cb = new SafeRegularTimeSeriesOfEnsemblePairsBuilder();
        try
        {
            cb.addData( firstBasisTime, first )
              .setTimeStep( Duration.ofHours( 1 ) )
              .addDataForBaseline( secondBasisTime, third )
              .setMetadata( meta )
              .setMetadataForBaseline( meta )
              .build();
            fail( "Expected a checked exception on building a time-series with a varying number of ensemble members "
                  + "in the baseline." );
        }
        catch ( MetricInputException e )
        {
            assertTrue( "Unexpected error message on checking member count.",
                        e.getMessage().equals( "While building a regular time-series of ensemble pairs: each pair "
                                               + "in the baseline must contain the same number of ensemble "
                                               + "members." ) );
        }

        //Check for exceptions on the iterators
        SafeRegularTimeSeriesOfEnsemblePairsBuilder d = new SafeRegularTimeSeriesOfEnsemblePairsBuilder();
        TimeSeriesOfEnsemblePairs ts =
                (TimeSeriesOfEnsemblePairs) d.addData( firstBasisTime, first )
                                             .setTimeStep( Duration.ofDays( 1 ) )
                                             .setMetadata( meta )
                                             .build();
        try
        {
            Iterator<TimeSeries<PairOfDoubleAndVectorOfDoubles>> it = ts.basisTimeIterator().iterator();
            it.forEachRemaining( a -> a.equals( null ) );
            it.next();
            fail( "Expected a checked exception on iterating a time-series with no more basis times left." );
        }
        catch ( NoSuchElementException e )
        {
        }
        try
        {
            Iterator<TimeSeries<PairOfDoubleAndVectorOfDoubles>> it = ts.durationIterator().iterator();
            it.forEachRemaining( a -> a.equals( null ) );
            it.next();
            fail( "Expected a checked exception on iterating a time-series with no more durations left." );
        }
        catch ( NoSuchElementException e )
        {
        }
        try
        {
            Iterator<TimeSeries<PairOfDoubles>> it = ts.ensembleTraceIterator().iterator();
            it.forEachRemaining( a -> a.equals( null ) );
            it.next();
            fail( "Expected a checked exception on iterating a time-series with no more ensemble traces left." );
        }
        catch ( NoSuchElementException e )
        {
        }
        try
        {
            Iterator<TimeSeries<PairOfDoubleAndVectorOfDoubles>> it = ts.basisTimeIterator().iterator();
            it.next();
            it.remove();
            fail( "Expected a checked exception on attempting to remove a basis time from an immutable time-series." );
        }
        catch ( UnsupportedOperationException e )
        {
        }
        try
        {
            Iterator<TimeSeries<PairOfDoubleAndVectorOfDoubles>> it = ts.durationIterator().iterator();
            it.next();
            it.remove();
            fail( "Expected a checked exception on attempting to remove a duration from an immutable time-series." );
        }
        catch ( UnsupportedOperationException e )
        {
        }
        try
        {
            Iterator<TimeSeries<PairOfDoubles>> it = ts.ensembleTraceIterator().iterator();
            it.next();
            it.remove();
            fail( "Expected a checked exception on attempting to remove an ensemble trace from an immutable "
                  + "time-series." );
        }
        catch ( UnsupportedOperationException e )
        {
        }
        //Check for null filters
        try
        {
            ts.filterByBasisTime( null );
            fail( "Expected a checked exception on attempting to filter by basis times with a null filter." );
        }
        catch ( NullPointerException e )
        {
        }
        try
        {
            ts.filterByDuration( null );
            fail( "Expected a checked exception on attempting to filter by duration with a null filter." );
        }
        catch ( NullPointerException e )
        {
        }
        //Attempt to build an irregular time-series via filtering
        try
        {
            List<PairOfDoubleAndVectorOfDoubles> values = new ArrayList<>();
            SafeRegularTimeSeriesOfEnsemblePairsBuilder e = new SafeRegularTimeSeriesOfEnsemblePairsBuilder();
            Instant basisTime = Instant.parse( "1985-01-01T00:00:00Z" );
            values.add( metIn.pairOf( 1, new double[] { 1 } ) );
            values.add( metIn.pairOf( 2, new double[] { 2 } ) );
            values.add( metIn.pairOf( 3, new double[] { 3 } ) );
            values.add( metIn.pairOf( 4, new double[] { 4 } ) );
            values.add( metIn.pairOf( 5, new double[] { 5 } ) );
            e.addData( basisTime, values ).setTimeStep( Duration.ofDays( 1 ) ).setMetadata( meta );
            e.build().filterByDuration( a -> a.equals( Duration.ofDays( 2 ) ) || a.equals( Duration.ofDays( 5 ) ) );
            fail( "Expected a checked exception on attempting to build an irregular time-series via a filter." );
        }
        catch ( UnsupportedOperationException e )
        {
        }
    }


    /**
     * Tests the {@link SafeRegularTimeSeriesOfEnsemblePairs#toString()} method.
     */

    @Test
    public void test10ToString()
    {
        List<PairOfDoubleAndVectorOfDoubles> values = new ArrayList<>();
        SafeRegularTimeSeriesOfEnsemblePairsBuilder b = new SafeRegularTimeSeriesOfEnsemblePairsBuilder();
        DataFactory metIn = DefaultDataFactory.getInstance();
        MetadataFactory metaFac = metIn.getMetadataFactory();
        Instant basisTime = Instant.parse( "1985-01-01T00:00:00Z" );
        for ( int i = 0; i < 5; i++ )
        {
            values.add( metIn.pairOf( i, new double[] { i, i + 1, i + 2 } ) );
        }
        Metadata meta = metaFac.getMetadata();
        b.addData( basisTime, values ).setTimeStep( Duration.ofDays( 1 ) ).setMetadata( meta );
        StringJoiner joiner = new StringJoiner( System.lineSeparator() );
        for ( int i = 0; i < 5; i++ )
        {
            joiner.add( "1985-01-0" + ( i + 2 )
                        + "T00:00:00Z"
                        + ",key: "
                        + ( i + 0.0 )
                        + " value: ["
                        + ( i + 0.0 )
                        + ","
                        + ( i + 1.0 )
                        + ","
                        + ( i + 2.0 )
                        + "]" );
        }

        //Check dataset count
        assertTrue( "Unexpected string representation of time-series.",
                    joiner.toString().equals( b.build().toString() ) );
        //Add another time-series
        Instant nextBasisTime = Instant.parse( "1985-01-02T00:00:00Z" );
        b.addData( nextBasisTime, values );
        for ( int i = 0; i < 5; i++ )
        {
            joiner.add( "1985-01-0" + ( i + 3 )
                        + "T00:00:00Z"
                        + ",key: "
                        + ( i + 0.0 )
                        + " value: ["
                        + ( i + 0.0 )
                        + ","
                        + ( i + 1.0 )
                        + ","
                        + ( i + 2.0 )
                        + "]" );
        }
        assertTrue( "Unexpected string representation of compound time-series.",
                    joiner.toString().equals( b.build().toString() ) );
    }

}
