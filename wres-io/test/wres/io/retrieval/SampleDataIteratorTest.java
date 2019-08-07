package wres.io.retrieval;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mock;

import java.time.Duration;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;
import org.mockito.Mockito;

import wres.config.generated.DateCondition;
import wres.config.generated.DurationUnit;
import wres.config.generated.IntBoundsType;
import wres.config.generated.PairConfig;
import wres.config.generated.PoolingWindowConfig;
import wres.config.generated.ProjectConfig;
import wres.io.project.Project;
import wres.util.CalculationException;

/**
 * Tests the {@link SampleDataIterator}
 *
 * @author james.brown@hydrosolved.com
 */

public class SampleDataIteratorTest
{

    /**
     * Tests that the {@link SampleDataIterator#getLeadBounds(int)} produces
     * two sets of bounds that match the expected lead duration bounds for 
     * system test scenario505.
     * @throws CalculationException if the retrieval of lead bounds fails unexpectedly
     */

    @Test
    public void testGetLeadBoundsReturnsTwoSetsOfBounds() throws CalculationException
    {
        // Mock the sufficient elements of the ProjectConfig
        IntBoundsType leadBoundsConfig = new IntBoundsType( 0, 40 );
        DateCondition issuedDatesConfig = new DateCondition( "2551-03-17T00:00:00Z", "2551-03-20T00:00:00Z" );
        PoolingWindowConfig leadTimesPoolingWindowConfig =
                new PoolingWindowConfig( 23, 17, DurationUnit.HOURS );
        PoolingWindowConfig issuedDatesPoolingWindowConfig =
                new PoolingWindowConfig( 13, 7, DurationUnit.HOURS );
        PairConfig pairsConfig = new PairConfig( null,
                                                 null,
                                                 null,
                                                 leadBoundsConfig,
                                                 null,
                                                 issuedDatesConfig,
                                                 null,
                                                 null,
                                                 null,
                                                 issuedDatesPoolingWindowConfig,
                                                 leadTimesPoolingWindowConfig,
                                                 null,
                                                 null );
        ProjectConfig mockedConfig =
                new ProjectConfig( null,
                                   pairsConfig,
                                   null,
                                   null,
                                   null,
                                   null );

        // Mock a concrete method in an abstract class
        // Thanks stackoverflow: https://stackoverflow.com/questions/1087339/using-mockito-to-test-abstract-classes
        SampleDataIterator iterator = Mockito.mock( SampleDataIterator.class, Mockito.CALLS_REAL_METHODS );

        // Mock the project wrapper
        Project project = mock( Project.class );
        when( project.getProjectConfig() ).thenReturn( mockedConfig );
        when( project.getLeadOffset( null ) ).thenReturn( Duration.ZERO );

        // Mock the return of the project wrapper by the class under test
        when( iterator.getProject() ).thenReturn( project );

        // Generate the expected bounds
        Pair<Duration, Duration> firstExpectedBounds = Pair.of( Duration.ofHours( 0 ), Duration.ofHours( 23 ) );
        Pair<Duration, Duration> secondExpectedBounds = Pair.of( Duration.ofHours( 17 ), Duration.ofHours( 40 ) );

        // Assert the expected bounds, of which there are two sets
        assertEquals( firstExpectedBounds, iterator.getLeadBounds( 0 ) );

        assertEquals( secondExpectedBounds, iterator.getLeadBounds( 1 ) );

        // No more bounds expected, but cannot assert this for now as some implementations 
        // of SampleDataIterator::calculateSamples create bounds that overflow before they
        // check and reject them
    }

    /**
     * Tests that the {@link SampleDataIterator#getLeadBounds(int)} produces
     * four sets of bounds that match the expected lead duration bounds for 
     * the shape of problem described in #63407-22.
     * @throws CalculationException if the retrieval of lead bounds fails unexpectedly
     */

    @Test
    public void testGetLeadBoundsReturnsFourSetsOfBounds() throws CalculationException
    {
        // Mock the sufficient elements of the ProjectConfig
        IntBoundsType leadBoundsConfig = new IntBoundsType( 0, 24 );
        PoolingWindowConfig leadTimesPoolingWindowConfig =
                new PoolingWindowConfig( 6, null, DurationUnit.HOURS );
        PairConfig pairsConfig = new PairConfig( null,
                                                 null,
                                                 null,
                                                 leadBoundsConfig,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 leadTimesPoolingWindowConfig,
                                                 null,
                                                 null );
        ProjectConfig mockedConfig =
                new ProjectConfig( null,
                                   pairsConfig,
                                   null,
                                   null,
                                   null,
                                   null );

        // Mock a concrete method in an abstract class
        // Thanks stackoverflow: https://stackoverflow.com/questions/1087339/using-mockito-to-test-abstract-classes
        SampleDataIterator iterator = Mockito.mock( SampleDataIterator.class, Mockito.CALLS_REAL_METHODS );

        // Mock the project wrapper
        Project project = mock( Project.class );
        when( project.getProjectConfig() ).thenReturn( mockedConfig );
        when( project.getLeadOffset( null ) ).thenReturn( Duration.ZERO );

        // Mock the return of the project wrapper by the class under test
        when( iterator.getProject() ).thenReturn( project );

        // Generate the expected bounds
        Pair<Duration, Duration> firstExpectedBounds = Pair.of( Duration.ofHours( 0 ), Duration.ofHours( 6 ) );
        Pair<Duration, Duration> secondExpectedBounds = Pair.of( Duration.ofHours( 6 ), Duration.ofHours( 12 ) );
        Pair<Duration, Duration> thirdExpectedBounds = Pair.of( Duration.ofHours( 12 ), Duration.ofHours( 18 ) );
        Pair<Duration, Duration> fourthExpectedBounds = Pair.of( Duration.ofHours( 18 ), Duration.ofHours( 24 ) );

        // Assert the expected bounds, of which there are two sets
        assertEquals( firstExpectedBounds, iterator.getLeadBounds( 0 ) );

        assertEquals( secondExpectedBounds, iterator.getLeadBounds( 1 ) );

        assertEquals( thirdExpectedBounds, iterator.getLeadBounds( 2 ) );

        assertEquals( fourthExpectedBounds, iterator.getLeadBounds( 3 ) );

        // No more bounds expected, but cannot assert this for now as some implementations 
        // of SampleDataIterator::calculateSamples create bounds that overflow before they
        // check and reject them
    }

}
