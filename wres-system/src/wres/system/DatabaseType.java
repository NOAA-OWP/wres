package wres.system;

/** The type of relational database management system. */
public enum DatabaseType
{
    MARIADB,
    POSTGRESQL,
    MYSQL,
    H2;
    
    /**
     * @return a lower case string representation of the enum.
     */
    @Override
    public String toString()
    {
        return super.toString().toLowerCase();
    }
}
