package wres.systests;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import org.junit.runner.Runner;
import org.junit.runner.Description;
import org.junit.runners.Suite;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A runner for executing classes in a test suite in random order.  
 * 
 * @author hank.herr
 */
public class SystemTestsSuiteRunner extends Suite
{
    private static final Logger LOGGER = LoggerFactory.getLogger( SystemTestsSuiteRunner.class );

    /** Super class constructor wrapper. */
    public SystemTestsSuiteRunner( Class<?> klass, RunnerBuilder builder )
            throws InitializationError
    {
        super( klass, builder );
    }

    /** Super class constructor wrapper. */
    public SystemTestsSuiteRunner( RunnerBuilder builder, Class<?>[] classes )
            throws InitializationError
    {
        super( builder, classes );
    }

    /** Super class constructor wrapper. */
    protected SystemTestsSuiteRunner( Class<?> klass, List<Runner> runners )
            throws InitializationError
    {
        super( klass, runners );
    }
    
    /**
     * Shuffle the order of the children, which comes from the order in
     * the test suite.
     */
    @Override
    protected List<Runner> getChildren() 
    {
        //Determine the seed. 
        long seed = System.nanoTime();
        if ( System.getProperty( "wres.systemTestSeed" ) != null )
        {
            LOGGER.info( "Found property specifying system test order seed; wres.systemTestSeed = " +
                         System.getProperty( "wres.systemTestSeed" ) );
            seed = Long.parseLong( System.getProperty( "wres.systemTestSeed" ) );
        }
        LOGGER.info( "The system testing seed used for class ordering is " + seed );
        Random rand = new Random( seed );

        //Get the current children, which are the classes in the suite.
        final List<Runner> children = super.getChildren();

        //Create a shuffled version in a new list, since the children list is not
        //allowed to be modified.
        ArrayList<Runner> shuffledChildren = new ArrayList<>();
        shuffledChildren.addAll( children );
        Collections.shuffle( shuffledChildren, rand );

        List<String> testOrder = shuffledChildren.stream()
                                                 .map( Runner::getDescription )
                                                 .map( Description::toString )
                                                 .collect( Collectors.toList() );

        LOGGER.info( "The tests will be executed in this order: {}. ", testOrder );

        return shuffledChildren;
    }
}
