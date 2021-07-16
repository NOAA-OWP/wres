package wres;

import java.nio.file.Path;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

public class ExecutionResult
{
    /**
     * The caller-supplied project name extracted during execution if available.
     */
    private final String name;

    /**
     * The project identity, if available.
     */

    private final String hash;

    /**
     * The exception that stopped execution if execution failed without Error.
     *
     * When an Error occurs, this should not be constructed and the Error should
     * propagate.
     */
    private final Exception exception;

    /**
     * The resources written.
     */
    private final Set<Path> resources;

    /**
     * Create a successful result.
     * @return an execution result
     */

    public static ExecutionResult success()
    {
        return new ExecutionResult( null, null, null, Set.of() );
    }

    /**
     * Create a successful result with a project name.
     * @param name the project name
     * @return an execution result
     */

    public static ExecutionResult success( String name )
    {
        return new ExecutionResult( name, null, null, Set.of() );
    }

    /**
     * Create a successful result with a project name and a collection of resources that were created.
     * @param name the project name
     * @param hash the hash of the project datasets
     * @param resources the resources created
     * @return an execution result
     * @throws NullPointerException if the set of resources is null
     */

    public static ExecutionResult success( String name, String hash, Set<Path> resources )
    {
        return new ExecutionResult( name, hash, null, resources );
    }

    /**
     * Create a unsuccessful result with a project name and an exception thrown.
     * @param name the project name
     * @param e the exception thrown
     * @return an execution result
     */

    public static ExecutionResult failure( String name, Exception e )
    {
        Objects.requireNonNull( e );
        return new ExecutionResult( name, null, e, Set.of() );
    }

    /**
     * Create a unsuccessful result with an exception thrown.
     * @param e the exception thrown
     * @return an execution result
     */

    public static ExecutionResult failure( Exception e )
    {
        Objects.requireNonNull( e );
        return new ExecutionResult( null, null, e, Set.of() );
    }

    /**
     * @return the name of the project declaration on which the evaluation is based or null
     */
    public String getName()
    {
        return this.name;
    }

    /**
     * @return the hash of the project datasets or null
     */
    public String getHash()
    {
        return this.hash;
    }

    /**
     * @return the exception thrown upon failure or null
     */
    public Exception getException()
    {
        return exception;
    }

    /**
     * @return the resources created during the execution
     */
    public Set<Path> getResources()
    {
        return this.resources; // Unmodifiable view
    }

    /**
     * Success?
     * @return True when the result was success, false when it was failure.
     */
    public boolean succeeded()
    {
        return !this.failed();
    }

    /**
     * Failure?
     * @return True when the result was failure, false when it was success.
     */
    public boolean failed()
    {
        Exception e = this.getException();
        return Objects.nonNull( e );
    }

    /**
     * Hidden constructor.
     * @param name the project name, if available
     * @param hash the hash of the project datasets, if available
     * @param exception an exception, if applicable
     * @param resources the resources created, not null
     */
    private ExecutionResult( String name,
                             String hash,
                             Exception exception,
                             Set<Path> resources )
    {
        Objects.requireNonNull( resources );

        this.name = name;
        this.hash = hash;
        this.exception = exception;
        this.resources = Collections.unmodifiableSet( new TreeSet<>( resources ) );
    }
}
