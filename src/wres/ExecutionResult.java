package wres;

import java.util.Objects;

public class ExecutionResult
{
    /**
     * The caller-supplied project name extracted during execution if available.
     */
    private final String name;

    /**
     * The exception that stopped execution if execution failed without Error.
     *
     * When an Error occurs, this should not be constructed and the Error should
     * propagate.
     */
    private final Exception exception;

    private ExecutionResult( String name,
                             Exception exception )
    {
        this.name = name;
        this.exception = exception;
    }

    public static ExecutionResult success()
    {
        return new ExecutionResult( null, null );
    }

    public static ExecutionResult success( String name )
    {
        return new ExecutionResult( name, null );
    }

    public static ExecutionResult failure( String name, Exception e )
    {
        Objects.requireNonNull( e );
        return new ExecutionResult( name, e );
    }

    public static ExecutionResult failure( Exception e )
    {
        Objects.requireNonNull( e );
        return new ExecutionResult( null, e );
    }

    public String getName()
    {
        return this.name;
    }

    public Exception getException()
    {
        return exception;
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
}
