package wres.tasker;

/**
 * When the count of evaluations in the wres.job queue is dangerously high, to
 * protect the system from problems, throw this Exception.
 */
class TooManyEvaluationsInQueueException extends RuntimeException
{
    TooManyEvaluationsInQueueException( String message )
    {
        super( message );
    }
}
