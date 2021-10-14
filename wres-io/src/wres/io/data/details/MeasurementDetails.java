package wres.io.data.details;

import java.sql.SQLException;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.io.utilities.DataProvider;
import wres.io.utilities.DataScripter;
import wres.io.utilities.Database;

/**
 * Details defining a unit of measurement within the database (i.e. CFS (cubic feet per second),
 * M (meter), etc)
 * @author Christopher Tubbs
 */
public final class MeasurementDetails extends CachedDetail<MeasurementDetails, String>
{
    private static final Logger
            LOGGER = LoggerFactory.getLogger( MeasurementDetails.class );

	private String unit = null;
	private Long measurementUnitID = null;

	/**
	 * Sets the name of the unit of measurement
	 * @param unit The new name of the unit of measurement
	 */
	public void setUnit(String unit)
	{
		if ( Objects.nonNull( unit ) && !unit.isBlank()
             && ( this.unit == null || !this.unit.equals( unit ) ) )
		{
            this.unit = unit;
			this.measurementUnitID = null;
		}
	}

	@Override
	public int compareTo(MeasurementDetails other) {
        return this.unit.compareTo( other.unit );
	}

	@Override
	public String getKey() {
        return this.unit;
	}

	@Override
	public Long getId() {
		return this.measurementUnitID;
	}

	@Override
	protected String getIDName() {
		return "measurementunit_id";
	}

	@Override
	public void setID( long id ) {
		this.measurementUnitID = id;
	}

    @Override
    protected Logger getLogger()
    {
        return MeasurementDetails.LOGGER;
    }

    @Override
    public String toString()
    {
        return this.unit;
    }

	@Override
	protected DataScripter getInsertSelect( Database database )
	{
        DataScripter script = new DataScripter( database );
        script.addLine( "INSERT INTO wres.MeasurementUnit ( unit_name ) ");
        script.addTab().addLine( "SELECT ?" );
		script.addArgument( this.unit );
        script.addTab().addLine( "WHERE NOT EXISTS" );
        script.addTab().addLine( "(" );
        script.addTab( 2 ).addLine( "SELECT 1" );
        script.addTab( 2 ).addLine( "FROM wres.MeasurementUnit" );
        script.addTab( 2 ).addLine( "WHERE unit_name = ?");
        script.addArgument( this.unit );
        script.addTab().addLine( ")" );
        script.setUseTransaction( true );
        script.retryOnSerializationFailure();
        script.retryOnUniqueViolation();
        script.setHighPriority( true );
		return script;
	}

    @Override
    public void save( Database database ) throws SQLException
    {
        DataScripter script = this.getInsertSelect( database );
        boolean performedInsert = script.execute() > 0;

        if ( performedInsert )
        {
            this.measurementUnitID = script.getInsertedIds()
                                           .get( 0 );
        }
        else
        {
            DataScripter scriptWithId = new DataScripter( database );
            scriptWithId.setHighPriority( true );
            scriptWithId.setUseTransaction( false );
            scriptWithId.addLine( "SELECT measurementunit_id" );
            scriptWithId.addLine( "FROM wres.MeasurementUnit" );
            scriptWithId.addLine( "WHERE unit_name = ? ");
            scriptWithId.addArgument( this.unit );
            scriptWithId.setMaxRows( 1 );

            try ( DataProvider data = scriptWithId.getData() )
            {
                this.update( data );
            }
        }

        LOGGER.trace( "Did I create MeasurementUnit ID {}? {}",
                      this.measurementUnitID,
                      performedInsert );
    }

	@Override
	protected Object getSaveLock()
	{
        // Locking is done in the insert/select on db side instead of here.
        return new Object();
	}
}
