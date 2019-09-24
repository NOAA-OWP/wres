package wres.systests;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.runner.Runner;
import org.junit.runners.Suite;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;

/**
 * A runner for executing classes in a test suite in randome orer.  
 * 
 * @author hank.herr
 *
 */
public class SystemTestsSuiteRunner extends Suite
{

    /** Super class constructor wrapper. */
    public SystemTestsSuiteRunner(Class<?> klass, RunnerBuilder builder)
            throws InitializationError {
        super(klass, builder);
    }

    /** Super class constructor wrapper. */
    public SystemTestsSuiteRunner(RunnerBuilder builder, Class<?>[] classes)
            throws InitializationError {
        super(builder, classes);
    }

    /** Super class constructor wrapper. */
    protected SystemTestsSuiteRunner(Class<?> klass, List<Runner> runners)
            throws InitializationError {
        super(klass, runners);
    }
    
    /**
     * Shuffle the order of the children, which comes from the order in
     * the test suite.
     */
    @Override
    protected List<Runner> getChildren() 
    {
        final List<Runner> children = super.getChildren();
        ArrayList<Runner> shuffledChildren = new ArrayList<>(); 
        shuffledChildren.addAll( children );
        Collections.shuffle(shuffledChildren);
        return shuffledChildren;
    }
}

