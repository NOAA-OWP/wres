package wres.io.retrieval;

/**
 * Policy for handling duplicates by valid time.
 * 
 * @author James Brown
 */

public enum DuplicatePolicy
{
    /** Keep all duplicates. */
    KEEP_ALL,

    /** Keep only one duplicate by valid time, namely the latest one by reference time. */
    KEEP_LATEST_REFERENCE_TIME,

    /** Keep only one duplicate by valid time, namely the earliest one by reference time. */
    KEEP_EARLIEST_REFERENCE_TIME;
}
