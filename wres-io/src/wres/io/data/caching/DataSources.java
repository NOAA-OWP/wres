package wres.io.data.caching;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.DataSourceConfig;
import wres.io.config.ConfigHelper;
import wres.io.config.OrderedSampleMetadata;
import wres.io.data.details.SourceDetails;
import wres.io.data.details.SourceDetails.SourceKey;
import wres.io.utilities.DataProvider;
import wres.io.utilities.DataScripter;
import wres.io.utilities.Database;
import wres.util.Collections;
import wres.util.TimeHelper;

/**
 * Caches information about the source of forecast and observation data
 * @author Christopher Tubbs
 */
public class DataSources extends Cache<SourceDetails, SourceKey>
{
    private static final int MAX_DETAILS = 10000;
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
	private static final DataSources instance = new DataSources();

	public DataSources()
    {
        this.initializeDetails();
    }

    private void populate(final DataProvider data)
    {
        if (data == null)
        {
            LOGGER.warn("The DataSources cache was populated with no data.");
            return;
        }

        SourceDetails detail;

        while ( data.next() )
        {
            detail = new SourceDetails();
            detail.setOutputTime( data.getString( "output_time" ) );
            detail.setSourcePath( data.getString( "path" ) );
            detail.setHash( data.getString( "hash" ) );
            detail.setIsPointData( data.getBoolean( "is_point_data" ) );
            detail.setID( data.getInt( "source_id" ) );

            this.getKeyIndex().put( detail.getKey(), detail.getId() );
            this.getDetails().put( detail.getId(), detail );
        }
    }

	private static DataSources getCache()
    {
        synchronized (CACHE_LOCK)
        {
            if ( instance.isEmpty())
            {
                DataSources.initialize();
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

    public static SourceDetails getById(Integer id)
    {
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

    public static SourceDetails getExistingSource(final String hash) throws SQLException
    {
        Objects.requireNonNull(hash, "A nonexistent hash was passed to DataSources#getActiveSourceID");

        SourceDetails sourceDetails = null;

        SourceKey key = new SourceKey( null, null, null, hash );

        if (DataSources.getCache().hasID( key ))
        {
            sourceDetails = DataSources.getCache().get(
                    DataSources.getCache().getID( key )
            );
        }

        if (sourceDetails == null)
        {
            DataScripter script = new DataScripter(  );
            script.setHighPriority( true );

            script.addLine("SELECT source_id, path, output_time::text, lead, hash, is_point_data");
            script.addLine("FROM wres.Source");
            script.addLine("WHERE hash = ?;");

            script.addArgument( hash );

            try (DataProvider data = script.getData())
            {
                while(data.next())
                {
                    sourceDetails = new SourceDetails();
                    sourceDetails.setHash( hash );
                    sourceDetails.setLead( data.getInt( "lead" ) );
                    sourceDetails.setOutputTime( data.getString( "output_time" ) );
                    sourceDetails.setSourcePath( data.getString( "path" ) );
                    sourceDetails.setID( data.getInt( "source_id" ) );
                    sourceDetails.setIsPointData( data.getBoolean( "is_point_data" ) );

                    DataSources.getCache().addElement( sourceDetails );
                }
            }
        }

        return sourceDetails;
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

            String script = "";

            script += "SELECT source_id, path, output_time::text, lead, hash, is_point_data" + NEWLINE;
            script += "FROM wres.Source" + NEWLINE;
            script += "WHERE hash = '" + hash + "';";

            try
            {
                connection = Database.getConnection();
                try (DataProvider data = Database.getResults( connection, script ))
                {
                    SourceDetails details;

                    if ( data.next() )
                    {
                        // TODO: Create a DataProvider constructor for SourceDetails
                        details = new SourceDetails();
                        details.setHash( hash );
                        details.setLead( data.getInt( "lead" ) );
                        details.setOutputTime( data.getString( "output_time" ) );
                        details.setSourcePath( data.getString( "path" ) );
                        details.setID( data.getInt( "source_id" ) );
                        details.setIsPointData( data.getBoolean( "is_point_data" ) );

                        DataSources.getCache().addElement( details );

                        id = data.getInt( "source_id" );
                    }
                }
            }
            finally
            {
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

	public static List<String> getSourcePaths( final OrderedSampleMetadata sampleMetadata,
                                               final DataSourceConfig dataSourceConfig)
            throws SQLException
    {
        List<String> paths = new ArrayList<>();

        boolean isForecast = ConfigHelper.isForecast( dataSourceConfig );

        DataScripter script = new DataScripter();
        script.addLine("SELECT path");
        script.addLine("FROM wres.Source S");
        script.addLine("WHERE S.is_point_data = FALSE");

        if ( sampleMetadata.getTimeWindow()
                           .getEarliestLeadDuration()
                           .equals( sampleMetadata.getTimeWindow().getLatestLeadDuration() ) )
        {
            script.addTab()
                  .addLine( "AND S.lead = ",
                            TimeHelper.durationToLead( sampleMetadata.getTimeWindow().getEarliestLeadDuration() ) );
        }
        else
        {
            script.addTab().addLine( "AND S.lead > ", TimeHelper.durationToLead( sampleMetadata.getMinimumLead() ) );
            script.addTab()
                  .addLine( "AND S.lead <= ",
                            TimeHelper.durationToLead( sampleMetadata.getTimeWindow().getLatestLeadDuration() ) );
        }

        if ( isForecast && sampleMetadata.getProjectDetails().getMinimumLead() > Integer.MIN_VALUE )
        {
            script.addTab().addLine( "AND S.lead >= ", sampleMetadata.getProjectDetails().getMinimumLead() );
        }

        if ( isForecast && sampleMetadata.getProjectDetails().getMaximumLead() < Integer.MAX_VALUE )
        {
            script.addTab().addLine("AND S.lead <= ", sampleMetadata.getProjectDetails().getMaximumLead());
        }

        if (sampleMetadata.getProjectDetails().getEarliestDate() != null)
        {
            script.addTab().add("AND S.output_time ");

            if (isForecast)
            {
                script.add("+ INTERVAL '1 ", TimeHelper.LEAD_RESOLUTION, "' * S.lead ");
            }

            script.addLine(">= '", sampleMetadata.getProjectDetails().getEarliestDate(), "'");
        }

        if (sampleMetadata.getProjectDetails().getLatestDate() != null)
        {
            script.addTab().add("AND S.output_time ");

            if (isForecast)
            {
                script.add("+ INTERVAL '1 ", TimeHelper.LEAD_RESOLUTION, "' * S.lead ");
            }

            script.addLine("<= '", sampleMetadata.getProjectDetails().getLatestDate(), "'");
        }

        String issueClause = null;
        if ( !sampleMetadata.getTimeWindow().hasUnboundedReferenceTimes() )
        {
            if ( sampleMetadata.getTimeWindow()
                               .getEarliestReferenceTime()
                               .equals( sampleMetadata.getTimeWindow().getLatestReferenceTime() ) )
            {
                issueClause = "S.output_time = '" + sampleMetadata.getTimeWindow().getEarliestReferenceTime()
                              + "'::timestamp without time zone ";
            }
            else
            {
                if ( !sampleMetadata.getTimeWindow().getEarliestReferenceTime().equals( Instant.MIN ) )
                {
                    // TODO: Uncomment when it's time to go exclusive-inclusive
                    //issueClause = "S.output_time > '" + timeWindow.getEarliestTime() + "'::timestamp without time zone ";
                    issueClause = "AND S.output_time >= '" + sampleMetadata.getTimeWindow().getEarliestReferenceTime()
                                  + "'::timestamp without time zone";
                }

                if ( !sampleMetadata.getTimeWindow().getLatestReferenceTime().equals( Instant.MAX ) )
                {
                    if ( issueClause == null )
                    {
                        issueClause = "";
                    }
                    else
                    {
                        issueClause += NEWLINE;
                    }

                    issueClause += "AND S.output_time <= '" + sampleMetadata.getTimeWindow().getLatestReferenceTime()
                                   + "'::timestamp without time zone";
                }
            }
        }

        if (issueClause != null)
        {
            script.addTab().addLine(issueClause);
        }

        if (issueClause == null && isForecast && sampleMetadata.getProjectDetails().getEarliestIssueDate() != null)
        {
            script.addTab().addLine("AND S.output_time >= '", sampleMetadata.getProjectDetails().getEarliestIssueDate(), "'");
        }

        if (issueClause == null && isForecast && sampleMetadata.getProjectDetails().getLatestIssueDate() != null)
        {
            script.addTab().addLine("AND S.output_time <= '", sampleMetadata.getProjectDetails().getLatestIssueDate(), "'");
        }

        script.addTab().addLine("AND EXISTS (");
        script.addTab(  2  ).addLine("SELECT 1");
        script.addTab(  2  ).addLine("FROM wres.ProjectSource PS");
        script.addTab(  2  ).addLine("WHERE PS.project_id = ", sampleMetadata.getProjectDetails().getId());
        script.addTab(   3   ).addLine("AND PS.member = ", sampleMetadata.getProjectDetails().getInputName( dataSourceConfig ));
        script.addTab(   3   ).addLine("AND PS.source_id = S.source_id");
        script.addTab().addLine(");");

        script.consume( pathRow -> paths.add(pathRow.getString( "path" )) );

        return paths;
    }


	@Override
	protected int getMaxDetails() {
		return MAX_DETAILS;
	}

	private static void initialize()
    {
        try
        {
            DataScripter script = new DataScripter(  );

            script.addLine("SELECT source_id,");
            script.addTab().addLine("path,");
            script.addTab().addLine("CAST(output_time AS TEXT) AS output_time,");
            script.addTab().addLine("hash,");
            script.addTab().addLine("is_point_data");
            script.addLine("FROM wres.Source");
            script.addLine("LIMIT ", MAX_DETAILS, ";");

            try (DataProvider sources = script.getData())
            {
                instance.populate( sources );
            }
        }
        catch (SQLException error)
        {
            // Failure to pre-populate cache should not affect primary outputs.
            LOGGER.warn( "An error was encountered when trying to populate the Source cache.",
                         error );
        }
    }
}
