package wres.system;

import javax.sql.DataSource;

import lombok.Getter;

/**
 * The type of relational database management system. The {@link DataSource} class names come from: 
 * <a href="https://github.com/brettwooldridge/HikariCP#popular-datasource-class-names">...</a>.
 */
@Getter
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
    private final boolean limitClauseSupported;

    /** Is {@code true} if the database supports a user function, {@code false} otherwise. */
    private final boolean userFunctionSupported;

    /** Is {@code true} if the database supports an analyze step, {@code false} otherwise. */
    private final boolean analyzeSupported;

    /**
     * @return {@code true} if the database supports a vacuum analyze step, {@code false} otherwise.
     */
    public boolean isVacuumAnalyzeSupported()
    {
        // Promote to constructor when required
        return this == DatabaseType.POSTGRESQL;
    }

    /**
     * @return {@code true} if the database supports a truncate cascade clause, {@code false} otherwise.
     */
    public boolean isTruncateCascadeSupported()
    {
        // Promote to constructor when required
        return this == DatabaseType.POSTGRESQL;
    }

    /**
     * Hidden constructor.
     *
     * @param dataSourceClassName the fully qualified data source class name, to be discovered on the class path
     * @param driverClassName the fully qualified driver name, to be discovered on the class path
     * @param limitClauseSupported is {@code true} if the database supports a {@code LIMIT} clause, {@code false} otherwise
     * @param userFunctionSupported is {@code true} if the database supports a user function, {@code false} otherwise
     * @param analyzeSupported is {@code true} if the database supports an analyze step, {@code false} otherwise
     */

    DatabaseType( String dataSourceClassName,
                  String driverClassName,
                  boolean limitClauseSupported,
                  boolean userFunctionSupported,
                  boolean analyzeSupported )
    {
        this.dataSourceClassName = dataSourceClassName;
        this.driverClassName = driverClassName;
        this.limitClauseSupported = limitClauseSupported;
        this.userFunctionSupported = userFunctionSupported;
        this.analyzeSupported = analyzeSupported;
    }
}
