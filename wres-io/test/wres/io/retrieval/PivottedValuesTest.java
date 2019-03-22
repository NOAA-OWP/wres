package wres.io.retrieval;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.TreeMap;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.TimeScaleFunction;
import wres.io.data.caching.Ensembles;
import wres.io.utilities.DataBuilder;
import wres.io.utilities.DataProvider;
import wres.util.TimeHelper;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore( { "javax.management.*", "javax.xml.*", "com.sun.*", "ch.qos.*", "org.slf4j.*" } )
public class PivottedValuesTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger( PivottedValues.class );

    /**
     * Tests the sorting of values by the order of ensembles returned from the Ensembles cache (mocked here)
     * <br><br>
     * <strong>Expected:</strong> with ensemble members listed in ascending order based on label,
     * the aggregated values should be in the order of ascending ensemble
     */
    @Test
    public void sortAggregatedValuesAscending()
    {
        Collection<Double> expectedValues = Arrays.asList(2.5, 6.5, 10.5);

        List<Pair<Integer, Double>> aggregatedValues = new ArrayList<>(  );
        aggregatedValues.add( Pair.of(1, 2.5) );
        aggregatedValues.add( Pair.of(2, 6.5));
        aggregatedValues.add( Pair.of(3, 10.5));

        try
        {
            this.initializeAscendingEnsembles();
        }
        catch ( Exception e )
        {
            Assert.fail("The test could not be properly prepared.");
        }

        PivottedValues pivottedValues = new PivottedValues( Instant.now(),
                                                            Duration.of( 10, TimeHelper.LEAD_RESOLUTION ),
                                                            this.getValueMapping() );

        Collection<Double> sorted = null;

        try
        {
            sorted = Whitebox.invokeMethod( pivottedValues, "sortAggregatedValues", aggregatedValues );
        }
        catch ( Exception e )
        {
            Assert.fail( "PivottedValues.sortAggregatedValues could not be called." );
        }

        if (!collectionsEqual( expectedValues, sorted ))
        {
            StringJoiner expectedJoiner = new StringJoiner( ",", "Expected: [", "]" );
            StringJoiner sortedJoiner = new StringJoiner( ",", "Sorted: [", "]" );

            expectedValues.forEach( value -> expectedJoiner.add(value.toString()) );
            sorted.forEach( value -> sortedJoiner.add(value.toString()) );
            LOGGER.error( expectedJoiner.toString() );
            LOGGER.error(sortedJoiner.toString());

            Assert.fail( "The sorted and expected values did not match." );
        }
    }

    private void initializeAscendingEnsembles() throws Exception
    {
        DataProvider data = DataBuilder.with("ensemble_name", "ensemblemember_id", "qualifier_id", "ensemble_id")
                                       .addRow("Test", "1", "", 1)
                                       .addRow("Test", "2", "", 2)
                                       .addRow("Test", "3", "", 3)
                                       .build();

        Ensembles ensembles = new Ensembles();
        Whitebox.invokeMethod( ensembles, "initializeDetails" );
        Whitebox.invokeMethod( ensembles, "populate", data );
        Whitebox.setInternalState( Ensembles.class, "INSTANCE", ensembles );
    }


    /**
     * Tests the sorting of values by the order of ensembles returned from the Ensembles cache (mocked here)
     * <br><br>
     * <strong>Expected:</strong> with ensemble members listed in descending order based on label,
     * the aggregated values should be in the order of descending ensemble
     */
    @Test
    public void sortAggregatedValuesDescending()
    {
        Collection<Double> expectedValues = Arrays.asList(10.5, 6.5, 2.5);

        List<Pair<Integer, Double>> aggregatedValues = new ArrayList<>(  );
        aggregatedValues.add( Pair.of(1, 2.5) );
        aggregatedValues.add( Pair.of(2, 6.5));
        aggregatedValues.add( Pair.of(3, 10.5));

        try
        {

            this.initializeDescendingEnsembles();
        }
        catch ( Exception e )
        {
            Assert.fail("The test could not be properly prepared.");
        }

        PivottedValues pivottedValues = new PivottedValues( Instant.now(),
                                                            Duration.of( 10, TimeHelper.LEAD_RESOLUTION ),
                                                            this.getValueMapping() );

        Collection<Double> sorted = null;

        try
        {
            sorted = Whitebox.invokeMethod( pivottedValues, "sortAggregatedValues", aggregatedValues );
        }
        catch ( Exception e )
        {
            Assert.fail( "PivottedValues.sortAggregatedValues could not be called." );
        }

        if (!collectionsEqual( expectedValues, sorted ))
        {
            StringJoiner expectedJoiner = new StringJoiner( ",", "Expected: [", "]" );
            StringJoiner sortedJoiner = new StringJoiner( ",", "Sorted: [", "]" );

            expectedValues.forEach( value -> expectedJoiner.add(value.toString()) );
            sorted.forEach( value -> sortedJoiner.add(value.toString()) );
            LOGGER.error( expectedJoiner.toString() );
            LOGGER.error(sortedJoiner.toString());

            Assert.fail( "The sorted and expected values did not match." );
        }
    }

    private void initializeDescendingEnsembles() throws Exception
    {
        DataProvider data = DataBuilder.with("ensemble_name", "ensemblemember_id", "qualifier_id", "ensemble_id")
                                       .addRow("Test", "3", "", 1)
                                       .addRow("Test", "2", "", 2)
                                       .addRow("Test", "1", "", 3)
                                       .build();

        Ensembles ensembles = new Ensembles();
        Whitebox.invokeMethod( ensembles, "initializeDetails" );
        Whitebox.invokeMethod( ensembles, "populate", data );
        Whitebox.setInternalState( Ensembles.class, "INSTANCE", ensembles );
    }

    /**
     * When no scaling is involved, all collected values should be in the list and
     * ordered by the member first and the position underneath the member second
     * <br><br>
     * <strong>Expected:</strong> each of the 12 values should be listed in the order
     * of member and position under the member
     */
    @Test
    public void sortDuplicateMemberValues()
    {
        Collection<Double> expected = Arrays.asList(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0);

        List<Pair<Integer, Double>> aggregatedValues = new ArrayList<>(  );
        aggregatedValues.add( Pair.of(1, 1.0) );
        aggregatedValues.add( Pair.of(1, 2.0));
        aggregatedValues.add(Pair.of(1, 3.0));
        aggregatedValues.add(Pair.of(1, 4.0));

        aggregatedValues.add(Pair.of(2, 5.0));
        aggregatedValues.add(Pair.of(2, 6.0));
        aggregatedValues.add(Pair.of(2, 7.0));
        aggregatedValues.add(Pair.of(2, 8.0));

        aggregatedValues.add(Pair.of(3, 9.0));
        aggregatedValues.add(Pair.of(3, 10.0));
        aggregatedValues.add(Pair.of(3, 11.0));
        aggregatedValues.add(Pair.of(3, 12.0));

        Map<PivottedValues.EnsemblePosition, List<Double>> valueMapping = new TreeMap<>(  );

        valueMapping.put( new PivottedValues.EnsemblePosition( 0, 1 ), Collections.singletonList( 1.0 ) );
        valueMapping.put( new PivottedValues.EnsemblePosition( 1, 1 ), Collections.singletonList( 2.0 ) );
        valueMapping.put( new PivottedValues.EnsemblePosition( 2, 1 ), Collections.singletonList( 3.0 ) );
        valueMapping.put( new PivottedValues.EnsemblePosition( 3, 1 ), Collections.singletonList( 4.0 ) );
        valueMapping.put( new PivottedValues.EnsemblePosition( 4, 2 ), Collections.singletonList( 5.0 ) );
        valueMapping.put( new PivottedValues.EnsemblePosition( 5, 2 ), Collections.singletonList( 6.0 ) );
        valueMapping.put( new PivottedValues.EnsemblePosition( 6, 2 ), Collections.singletonList( 7.0 ) );
        valueMapping.put( new PivottedValues.EnsemblePosition( 7, 2 ), Collections.singletonList( 8.0 ) );
        valueMapping.put( new PivottedValues.EnsemblePosition( 8, 3 ), Collections.singletonList( 9.0 ) );
        valueMapping.put( new PivottedValues.EnsemblePosition( 9, 3 ), Collections.singletonList( 10.0 ) );
        valueMapping.put( new PivottedValues.EnsemblePosition( 10, 3 ), Collections.singletonList( 11.0 ) );
        valueMapping.put( new PivottedValues.EnsemblePosition( 11, 3 ), Collections.singletonList( 12.0 ) );

        PivottedValues pivottedValues =
                new PivottedValues( Instant.now(), Duration.of( 10, TimeHelper.LEAD_RESOLUTION ), valueMapping );

        try
        {

            this.initializeAscendingEnsembles();
        }
        catch ( Exception e )
        {
            Assert.fail("The test could not be properly prepared.");
        }

        Collection<Double> sorted = null;

        try
        {
            sorted = Whitebox.invokeMethod( pivottedValues, "sortAggregatedValues", aggregatedValues );
        }
        catch ( Exception e )
        {
            Assert.fail( "PivottedValues.sortAggregatedValues could not be called." );
        }

        if (!collectionsEqual( expected, sorted ))
        {
            StringJoiner expectedJoiner = new StringJoiner( ",", "Expected: [", "]" );
            StringJoiner sortedJoiner = new StringJoiner( ",", "Sorted: [", "]" );

            expected.forEach( value -> expectedJoiner.add(value.toString()) );
            sorted.forEach( value -> sortedJoiner.add(value.toString()) );
            LOGGER.error( expectedJoiner.toString() );
            LOGGER.error(sortedJoiner.toString());

            Assert.fail( "The sorted and expected values did not match." );
        }
    }

    /**
     * When values for members are scaled, the resulting collection of values
     * should be each aggregated value in member order
     * <br><br>
     * <strong>Expected:</strong> the aggregated value for each member should be in the order of the members
     */
    @Test
    public void getScaledValues()
    {
        Double[] expectedValues = new Double[]{2.5, 6.5, 10.5};

        try
        {
            this.initializeAscendingEnsembles();
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            Assert.fail("The test could not be properly prepared.");
        }

        Map<PivottedValues.EnsemblePosition, List<Double>> valueMapping = new TreeMap<>(  );

        List<Double> member1 = new ArrayList<>(  );
        member1.add( 1.0 );
        member1.add( 2.0 );
        member1.add( 3.0 );
        member1.add( 4.0 );

        List<Double> member2 = new ArrayList<>();
        member2.add(5.0);
        member2.add(6.0);
        member2.add(7.0);
        member2.add(8.0);

        List<Double> member3 = new ArrayList<>(  );
        member3.add(9.0);
        member3.add(10.0);
        member3.add(11.0);
        member3.add(12.0);

        valueMapping.put( new PivottedValues.EnsemblePosition( 0, 1 ), member1 );
        valueMapping.put(new PivottedValues.EnsemblePosition( 1, 2 ), member2);
        valueMapping.put(new PivottedValues.EnsemblePosition( 2, 3 ), member3);

        PivottedValues pivottedValues =
                new PivottedValues( Instant.now(), Duration.of( 10, TimeHelper.LEAD_RESOLUTION ), valueMapping );

        Double[] aggregatedValues = pivottedValues.getAggregatedValues( true, TimeScaleFunction.MEAN);

        if (!this.collectionsEqual( Arrays.asList(expectedValues), Arrays.asList(aggregatedValues) ))
        {
            StringJoiner expectedJoiner = new StringJoiner( ",", "Expected: [", "]" );
            StringJoiner sortedJoiner = new StringJoiner( ",", "Sorted: [", "]" );

            Arrays.asList(expectedValues).forEach( value -> expectedJoiner.add(value.toString()) );
            Arrays.asList(aggregatedValues).forEach( value -> sortedJoiner.add(value.toString()) );
            LOGGER.error( expectedJoiner.toString() );
            LOGGER.error(sortedJoiner.toString());

            Assert.fail( "The sorted and expected values did not match." );
        }
    }

    /**
     * When the collected values are not scaled, each value should be present in
     * the final collection in order of ensemble and position in list
     */
    @Test
    public void getDuplicateMemberValues()
    {
        Double[] expected = new Double[]{1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0};
        Map<PivottedValues.EnsemblePosition, List<Double>> valueMapping = new TreeMap<>(  );

        valueMapping.put( new PivottedValues.EnsemblePosition( 0, 1 ), Collections.singletonList( 1.0 ) );
        valueMapping.put( new PivottedValues.EnsemblePosition( 1, 1 ), Collections.singletonList( 2.0 ) );
        valueMapping.put( new PivottedValues.EnsemblePosition( 2, 1 ), Collections.singletonList( 3.0 ) );
        valueMapping.put( new PivottedValues.EnsemblePosition( 3, 1 ), Collections.singletonList( 4.0 ) );
        valueMapping.put( new PivottedValues.EnsemblePosition( 4, 2 ), Collections.singletonList( 5.0 ) );
        valueMapping.put( new PivottedValues.EnsemblePosition( 5, 2 ), Collections.singletonList( 6.0 ) );
        valueMapping.put( new PivottedValues.EnsemblePosition( 6, 2 ), Collections.singletonList( 7.0 ) );
        valueMapping.put( new PivottedValues.EnsemblePosition( 7, 2 ), Collections.singletonList( 8.0 ) );
        valueMapping.put( new PivottedValues.EnsemblePosition( 8, 3 ), Collections.singletonList( 9.0 ) );
        valueMapping.put( new PivottedValues.EnsemblePosition( 9, 3 ), Collections.singletonList( 10.0 ) );
        valueMapping.put( new PivottedValues.EnsemblePosition( 10, 3 ), Collections.singletonList( 11.0 ) );
        valueMapping.put( new PivottedValues.EnsemblePosition( 11, 3 ), Collections.singletonList( 12.0 ) );

        try
        {
            this.initializeAscendingEnsembles();
        }
        catch ( Exception e )
        {
            Assert.fail("The test could not be properly prepared.");
        }

        PivottedValues pivottedValues =
                new PivottedValues( Instant.now(), Duration.of( 10, TimeHelper.LEAD_RESOLUTION ), valueMapping );

        Double[] aggregatedValues = pivottedValues.getAggregatedValues( false, TimeScaleFunction.MEAN);

        if (!this.collectionsEqual( Arrays.asList(expected), Arrays.asList(aggregatedValues) ))
        {
            StringJoiner expectedJoiner = new StringJoiner( ",", "Expected: [", "]" );
            StringJoiner sortedJoiner = new StringJoiner( ",", "Sorted: [", "]" );

            Arrays.asList(expected).forEach( value -> expectedJoiner.add(value.toString()) );
            Arrays.asList(aggregatedValues).forEach( value -> sortedJoiner.add(value.toString()) );
            LOGGER.error( expectedJoiner.toString() );
            LOGGER.error(sortedJoiner.toString());

            Assert.fail( "The sorted and expected values did not match." );
        }
    }

    /**
     * Determines whether or not two collections of doubles are equal
     * @param control The expected collection
     * @param experiment The experimental collection that should match the control
     * @return Whether or not the two collections are equal
     */
    private boolean collectionsEqual(final Iterable<Double> control, final Iterable<Double> experiment)
    {
        final Double EPSILON = 0.00001;
        Iterator<Double> controlValues = control.iterator();
        Iterator<Double> experimentValues = experiment.iterator();

        while (controlValues.hasNext() && experimentValues.hasNext())
        {
            Double controlValue = controlValues.next();
            Double experimentValue = experimentValues.next();

            if (Math.abs(controlValue - experimentValue) > EPSILON)
            {
                return false;
            }
        }

        return !(controlValues.hasNext() || experimentValues.hasNext());
    }

    /**
     * Creates a consistent mapping of values to test against
     * <br><br>
     * Mapped data is:
     * <ul>
     *     <li>1: [1.0, 2.0, 3.0, 4.0]</li>
     *     <li>2: [5.0, 6.0, 7.0, 8.0]</li>
     *     <li>3: [9.0, 10.0, 11.0, 12.0]</li>
     * </ul>
     * @return A mapping of values to test against
     */
    private Map<PivottedValues.EnsemblePosition, List<Double>> getValueMapping()
    {
        Map<PivottedValues.EnsemblePosition, List<Double>> valueMapping = new TreeMap<>(  );

        List<Double> member1 = new ArrayList<>(  );
        member1.add( 1.0 );
        member1.add( 2.0 );
        member1.add( 3.0 );
        member1.add( 4.0 );

        List<Double> member2 = new ArrayList<>();
        member2.add(5.0);
        member2.add(6.0);
        member2.add(7.0);
        member2.add(8.0);

        List<Double> member3 = new ArrayList<>(  );
        member3.add(9.0);
        member3.add(10.0);
        member3.add(11.0);
        member3.add(12.0);

        valueMapping.put( new PivottedValues.EnsemblePosition( 0, 1 ), member1 );
        valueMapping.put(new PivottedValues.EnsemblePosition( 1, 2 ), member2);
        valueMapping.put(new PivottedValues.EnsemblePosition( 2, 3 ), member3);

        return valueMapping;
    }
}
