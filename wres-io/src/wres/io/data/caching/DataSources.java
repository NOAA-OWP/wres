package wres.io.data.caching;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.DataSourceConfig;
import wres.io.config.ConfigHelper;
import wres.io.data.details.ProjectDetails;
import wres.io.data.details.SourceDetails;
import wres.io.data.details.SourceDetails.SourceKey;
import wres.io.utilities.Database;
import wres.io.utilities.ScriptBuilder;
import wres.util.Collections;
import wres.util.TimeHelper;

/**
 * Caches information about the source of forecast and observation data
 * @author Christopher Tubbs
 */
public class DataSources extends Cache<SourceDetails, SourceKey>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(DataSources.class);
    private static final Object CACHE_LOCK = new Object();

    private static final Object DETAIL_LOCK = new Object();
    private static final Object KEY_LOCK = new Object();

    @Override
    protected Object getDetailLock()
    {
        return DataSources.DETAIL_LOCK;
    }

    @Override
    protected Object getKeyLock()
    {
        return DataSources.KEY_LOCK;
    }

    /**
     * Global Cache of basic source data
     */
	private static DataSources instance = null;

	private static DataSources getCache()
    {
        synchronized (CACHE_LOCK)
        {
            if ( instance == null)
            {
                instance = new DataSources();
                instance.init();
            }
            return instance;
        }
    }
	
	/**
	 * Gets the ID of source metadata from the global cache based on a file path and the date of its output
	 * @param path The path to the file on the file system
	 * @param outputTime The time in which the information was generated
	 * @param lead the lead time
	 * @param hash the hash code for the source file
	 * @return The ID of the source in the database
	 * @throws SQLException Thrown when interaction with the database failed
	 */
	public static Integer getSourceID(String path, String outputTime, Integer lead, String hash) throws SQLException
    {
		return getCache().getID(path, outputTime, lead, hash);
	}

	public static SourceDetails get(String path, String outputTime, Integer lead, String hash) throws SQLException
    {
        int id = DataSources.getCache().getID( path, outputTime, lead, hash );
        return DataSources.getCache().get( id );
    }

    public static boolean isCached( SourceDetails.SourceKey key )
    {
        return DataSources.getCache()
                          .hasID( key );
    }

	public static boolean hasSource(String hash)
            throws SQLException
    {
        return DataSources.getActiveSourceID( hash ) != null;
    }

    public static String getHash(int sourceId)
    {
        String hash = null;

        SourceKey key = Collections.getKeyByValue( DataSources.getCache().getKeyIndex(), sourceId );

        if (key != null)
        {
            hash = key.getHash();
        }

        return hash;
    }

    public static Integer getActiveSourceID(String hash)
            throws SQLException
    {
        Objects.requireNonNull(hash, "A nonexistent hash was passed to DataSources#getActiveSourceID");

        Integer id = null;

        SourceKey key = new SourceKey( null, null, null, hash );

        if (DataSources.getCache().hasID( key ))
        {
            id = DataSources.getCache().getID( key );
        }

        if (id == null)
        {
            Connection connection = null;
            ResultSet results = null;

            String script = "";

            script += "SELECT source_id, path, output_time::text, lead, hash" + NEWLINE;
            script += "FROM wres.Source" + NEWLINE;
            script += "WHERE hash = '" + hash + "';";

            try
            {
                connection = Database.getConnection();
                results = Database.getResults( connection, script );

                SourceDetails details;

                if (results.next())
                {
                    details = new SourceDetails(  );
                    details.setHash( hash );
                    details.setLead( results.getInt( "lead" ) );
                    details.setOutputTime( results.getString("output_time") );
                    details.setSourcePath( results.getString( "path" ) );
                    details.setID( results.getInt( "source_id" ) );

                    DataSources.getCache().addElement( details );

                    id = results.getInt( "source_id" );
                }
                else
                {
                    throw new SQLException("No source could be found with the hash of '" + hash + "'");
                }

            }
            finally
            {
                if (results != null)
                {
                    try
                    {
                        results.close();
                    }
                    catch ( SQLException se )
                    {
                        // Exception on close should not affect primary outputs.
                        LOGGER.warn( "Failed to close result set {}.", results, se );
                    }
                }

                if (connection != null)
                {
                    Database.returnConnection( connection );
                }
            }
        }

        return id;
    }

    public static void put( SourceDetails sourceDetails )
    {
        DataSources.getCache().add( sourceDetails );
    }
	
	/**
	 * Gets the ID of source metadata from the instanced cache based on a file path and the date of its output
	 * @param path The path to the file on the file system
	 * @param outputTime The time in which the information was generation
	 * @param lead the lead time
	 * @param hash the hash code for the source file
	 * @return The ID of the source in the database
	 * @throws SQLException Thrown when interaction with the database failed
	 */
	Integer getID(String path, String outputTime, Integer lead, String hash) throws SQLException
    {
		return this.getID(SourceDetails.createKey(path, outputTime, lead, hash));
	}
	
	@Override
    public Integer getID(SourceKey key) throws SQLException
    {
	    if (!this.hasID(key))
	    {
	        addElement(new SourceDetails(key));
	    }

	    return super.getID(key);
	}

	public static List<String> getSourcePaths( final ProjectDetails projectDetails,
                                               final DataSourceConfig dataSourceConfig,
                                               final String firstIssueDate,
                                               final String lastIssueDate,
                                               final Integer firstLead,
                                               final Integer lastLead)
            throws SQLException
    {
        List<String> paths = new ArrayList<>();

        boolean isForecast = ConfigHelper.isForecast( dataSourceConfig );

        ScriptBuilder script = new ScriptBuilder();
        script.addLine("SELECT path");
        script.addLine("FROM wres.Source S");
        script.addLine("WHERE S.is_point_data = FALSE");

        if (isForecast && firstLead != null)
        {
            script.addTab().addLine("AND S.lead > ", firstLead);
        }

        if (isForecast && lastLead != null)
        {
            script.addTab().addLine("AND S.lead <= ", lastLead);
        }

        if (isForecast && projectDetails.getMinimumLeadHour() > Integer.MIN_VALUE)
        {
            script.addTab().addLine("AND S.lead >= ", projectDetails.getMinimumLeadHour());
        }

        if (isForecast && projectDetails.getMaximumLeadHour() < Integer.MAX_VALUE)
        {
            script.addTab().addLine("AND S.lead <= ", projectDetails.getMaximumLeadHour());
        }

        if (projectDetails.getEarliestDate() != null)
        {
            script.addTab().add("AND S.output_time ");

            if (isForecast)
            {
                script.add("+ INTERVAL '1 ", TimeHelper.LEAD_RESOLUTION, "' * S.lead ");
            }

            script.addLine(">= '", projectDetails.getEarliestDate(), "'");
        }

        if (projectDetails.getLatestDate() != null)
        {
            script.addTab().add("AND S.output_time ");

            if (isForecast)
            {
                script.add("+ INTERVAL '1 ", TimeHelper.LEAD_RESOLUTION, "' * S.lead ");
            }

            script.addLine("<= '", projectDetails.getLatestDate(), "'");
        }

        // TODO: This is just the first sweep for issue time pooling support
        if (firstIssueDate != null)
        {
            script.addTab().addLine("AND S.output_time >= '", firstIssueDate, "'");
        }
        else if (isForecast && projectDetails.getEarliestIssueDate() != null)
        {
            script.addTab().addLine("AND S.output_time >= '", projectDetails.getEarliestIssueDate(), "'");
        }

        if (lastIssueDate != null)
        {
            script.addTab().addLine("AND S.output_time <= '", lastIssueDate, "'");
        }
        else if (isForecast && projectDetails.getLatestIssueDate() != null)
        {
            script.addTab().addLine("AND S.output_time <= '", projectDetails.getLatestIssueDate(), "'");
        }

        script.addTab().addLine("AND EXISTS (");
        script.addTab(  2  ).addLine("SELECT 1");
        script.addTab(  2  ).addLine("FROM wres.ProjectSource PS");
        script.addTab(  2  ).addLine("WHERE PS.project_id = ", projectDetails.getId());
        script.addTab(   3   ).addLine("AND PS.member = ", projectDetails.getInputName( dataSourceConfig ));
        script.addTab(   3   ).addLine("AND PS.source_id = S.source_id");
        script.addTab().addLine(");");

        script.consume( pathRow -> paths.add(pathRow.getString( "path" )) );

        return paths;
    }


	@Override
	protected int getMaxDetails() {
		return 1000;
	}

    @Override
    protected synchronized void init()
    {
        // Exit if there are details populated and the keys and details are synced
        if (getKeyIndex().size() > 0) {
            this.clearCache();
        }

        Connection connection = null;
        ResultSet sources = null;
        this.initializeDetails();

        try
        {
            connection = Database.getHighPriorityConnection();
            String loadScript = "SELECT source_id, path, CAST(output_time AS TEXT) AS output_time, hash" + System.lineSeparator();
            loadScript += "FROM wres.Source" + System.lineSeparator();
            loadScript += "LIMIT " + getMaxDetails();
            
            sources = Database.getResults(connection, loadScript);
            SourceDetails detail;
            
            while (sources.next()) {
                detail = new SourceDetails();
                detail.setOutputTime(sources.getString("output_time"));
                detail.setSourcePath(sources.getString("path"));
                detail.setHash( sources.getString( "hash" ) );
                detail.setID( sources.getInt( "source_id" ) );
                
                this.getKeyIndex().put(detail.getKey(), detail.getId());
                this.getDetails().put(detail.getId(), detail);
            }
        }
        catch (SQLException error)
        {
            // Failure to pre-populate cache should not affect primary outputs.
            LOGGER.warn( "An error was encountered when trying to populate the Source cache.",
                         error );
        }
        finally
        {
            if (sources != null)
            {
                try
                {
                    sources.close();
                }
                catch(SQLException e)
                {
                    // Exception on close should not affect primary outputs.
                    LOGGER.warn( "An error was encountered when trying to close the resultset that contained data source information.",
                                  e );
                }
            }

            if (connection != null)
            {
                Database.returnHighPriorityConnection(connection);
            }
        }
    }
}
