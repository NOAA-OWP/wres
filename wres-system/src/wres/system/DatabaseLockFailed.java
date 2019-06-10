package wres.system;

import java.util.Objects;

class DatabaseLockFailed extends RuntimeException
{
    enum Operation
    {
        LOCK_SHARED,
        LOCK_EXCLUSIVE,
        UNLOCK_SHARED,
        UNLOCK_EXCLUSIVE
    }

    DatabaseLockFailed( Integer prefix,
                        Integer lockName,
                        Operation operation )
    {
        super( "Another WRES instance is performing a conflicting function. "
               + "Failed to lock|unlock with prefix=" + prefix
               + ", lockName=" + lockName
               + ", operation=" + operation );
        Objects.requireNonNull( prefix );
        Objects.requireNonNull( lockName );
        Objects.requireNonNull( operation );
    }
}
