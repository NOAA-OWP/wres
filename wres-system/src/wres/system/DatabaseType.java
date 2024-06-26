package wres.system;

import javax.sql.DataSource;
import java.sql.Driver;

/**
 * The type of relational database management system. The {@link DataSource} class names come from: 
 * <a href="https://github.com/brettwooldridge/HikariCP#popular-datasource-class-names">...</a>.
 */
public enum DatabaseType
{   
    /** Postgres is supported and recommended. */
    POSTGRESQL( "org.postgresql.ds.PGSimpleDataSource", "org.postgresql.Driver", true, false, true ),

    /** H2 has experimental support in in-memory mode and is used in tests. */
    H2( "org.h2.jdbcx.JdbcDataSource", "org.h2.Driver", true, true, true ),

    /** Code changes and testing are needed to support MySQL. Uses the same {@link DataSource} as {@link #MARIADB}. */
    MYSQL( "org.mariadb.jdbc.MariaDbDataSource", "org.mariadb.jdbc.Driver", true, true, false ),

    /** Code changes and testing are needed to support MariaDB. */
    MARIADB( "org.mariadb.jdbc.MariaDbDataSource", "org.mariadb.jdbc.Driver", true, true, false ),

    /** Code changes and testing are needed to support SQLite. */
    SQLITE( "org.sqlite.SQLiteDataSource", "org.sqlite.JDBC", true, false, false );

    /**
     * Liquibase changes or "clean" or "remove orphans" should use
     * exclusive lock on this. Any and every ingest/evaluation should first get
     * a shared lock on this, except those mentioned above, which should get it
     * exclusively.
     */
    public static final Long SHARED_READ_OR_EXCLUSIVE_DESTROY_NAME = 1L;
    
    /** The fully qualified data source class name, to be discovered on the class path. **/
    private final String dataSourceClassName;

    /** The fully qualified driver class name, to be discovered on the class path. **/
    private final String driverClassName;

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
     * @return the fully qualified class name of the {@link DataSource}.
     */
    public String getDataSourceClassName()
    {
        return this.dataSourceClassName;
    }

    /**
     * @return the fully qualified name of the database {@link Driver}.
     */
    public String getDriverClassName()
    {
        return this.driverClassName;
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
    public boolean hasAnalyze()
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
     * @param dataSourceClassName the fully qualified data source class name, to be discovered on the class path
     * @param driverClassName the fully qualified driver name, to be discovered on the class path
     * @param hasLimit is {@code true} if the database supports a {@code LIMIT} clause, {@code false} otherwise
     * @param hasUser is {@code true} if the database supports a user function, {@code false} otherwise
     * @param hasAnalyze is {@code true} if the database supports an analyze step, {@code false} otherwise
     */

    private DatabaseType( String dataSourceClassName,
                          String driverClassName,
                          boolean hasLimit,
                          boolean hasUser,
                          boolean hasAnalyze )
    {
        this.dataSourceClassName = dataSourceClassName;
        this.driverClassName = driverClassName;
        this.hasLimit = hasLimit;
        this.hasUser = hasUser;
        this.hasAnalyze = hasAnalyze;
    }
}
