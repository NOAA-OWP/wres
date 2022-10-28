package wres.system;

import java.util.Objects;

public class DatabaseLockFailed extends RuntimeException
{
    private static final long serialVersionUID = -8951384634940360610L;

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
