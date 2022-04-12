package wres.system;

/** The type of relational database management system. */
public enum DatabaseType
{
    /** Postgres is supported and recommended. */
    POSTGRESQL,

    /** H2 has experimental support in in-memory mode and is used in tests. */
    H2,

    /** Code changes and testing are needed to support MySQL. */
    MYSQL,

    /** Code changes and testing are needed to support MariaDB. */
    MARIADB,

    /** Code changes and testing are needed to support SQLite. */
    SQLITE;

    /**
     * @return a lower case string representation of the enum.
     */
    @Override
    public String toString()
    {
        return super.toString().toLowerCase();
    }
}
