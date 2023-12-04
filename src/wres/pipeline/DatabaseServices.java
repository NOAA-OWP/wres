package wres.pipeline;

import wres.io.database.Database;
import wres.io.database.locking.DatabaseLockManager;

/**
 * Small value object to collate a {@link Database} with an {@link DatabaseLockManager}. This may be disaggregated
 * for transparency if we can reduce the number of input arguments to some methods.
 * @param database The database instance.
 * @param databaseLockManager The Database lock manager instance.
 */

record DatabaseServices( Database database, DatabaseLockManager databaseLockManager )
{
}
