package wres.system;

/** 
 * The type of relational database management system. The {@code DataSource} class names come from: 
 * https://github.com/brettwooldridge/HikariCP#popular-datasource-class-names.
 */
public enum DatabaseType
{
    /** Postgres is supported and recommended. */
    POSTGRESQL( "org.postgresql.ds.PGSimpleDataSource", true, false, true ),

    /** H2 has experimental support in in-memory mode and is used in tests. */
    H2( "org.h2.jdbcx.JdbcDataSource", true, true, true ),

    /** 
     * Code changes and testing are needed to support MySQL. Also, see the note from: 
     * https://github.com/brettwooldridge/HikariCP#popular-datasource-class-names regarding network timeout support.
     */
    MYSQL( "com.mysql.jdbc.jdbc2.optional.MysqlDataSource", true, true, false ),

    /** Code changes and testing are needed to support MariaDB. */
    MARIADB( "org.mariadb.jdbc.MariaDbDataSource", true, true, false ),

    /** Code changes and testing are needed to support SQLite. */
    SQLITE( "org.sqlite.SQLiteDataSource", true, false, false );

    /** The fully qualified data source class name, to be discovered on the class path. **/
    private final String driverName;

    /** Is {@code true} if the database supports a {@code LIMIT} clause, {@code false} otherwise. */
    private final boolean hasLimit;

    /** Is {@code true} if the database supports a user function, {@code false} otherwise. */
    private final boolean hasUser;

    /** Is {@code true} if the database supports an analyze step, {@code false} otherwise. */
    private final boolean hasAnalyze;

    /**
     * @return a lower case string representation of the enum.
     */
    @Override
    public String toString()
    {
        return super.toString().toLowerCase();
    }

    /**
     * @return {@code true} if the database supports a {@code LIMIT} clause, {@code false} otherwise.
     */
    public String getDriverName()
    {
        return this.driverName;
    }

    /**
     * @return {@code true} if the database supports a user function, {@code false} otherwise.
     */
    public boolean hasUserFunction()
    {
        return this.hasUser;
    }

    /**
     * @return {@code true} if the database supports a {@code LIMIT} clause, {@code false} otherwise.
     */
    public boolean hasLimitClause()
    {
        return this.hasLimit;
    }

    /**
     * @return {@code true} if the database supports an analyze step, {@code false} otherwise.
     */
    public boolean hasAnalyzeStep()
    {
        return this.hasAnalyze;
    }

    /**
     * @return {@code true} if the database supports a vacuum analyze step, {@code false} otherwise.
     */
    public boolean hasVacuumAnalyze()
    {
        // Promote to constructor when required
        return this == DatabaseType.POSTGRESQL;
    }

    /**
     * @return {@code true} if the database supports a truncate cascade clause, {@code false} otherwise.
     */
    public boolean hasTruncateCascade()
    {
        // Promote to constructor when required
        return this == DatabaseType.POSTGRESQL;
    }

    /**
     * Hidden constructor.
     * 
     * @param driverName the fully qualified driver class name, to be discovered on the class path
     * @param hasLimit is {@code true} if the database supports a {@code LIMIT} clause, {@code false} otherwise
     * @param hasUser is {@code true} if the database supports a user function, {@code false} otherwise
     * @param hasAnalyze is {@code true} if the database supports an analyze step, {@code false} otherwise
     */

    private DatabaseType( String driverName, boolean hasLimit, boolean hasUser, boolean hasAnalyze )
    {
        this.driverName = driverName;
        this.hasLimit = hasLimit;
        this.hasUser = hasUser;
        this.hasAnalyze = hasAnalyze;
    }
}
