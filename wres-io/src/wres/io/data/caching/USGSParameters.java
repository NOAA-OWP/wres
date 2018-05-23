package wres.io.data.caching;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.commons.lang3.StringUtils;

import wres.io.utilities.ScriptBuilder;

public class USGSParameters
{
    private USGSParameters() {}

    private static class ParameterKey implements Comparable<ParameterKey>
    {
        ParameterKey(String name, String measurementUnit, String aggregation)
        {
            this.name = name;
            this.measurementUnit = measurementUnit;
            this.aggregation = aggregation;
        }

        private final String name;
        private final String measurementUnit;
        private final String aggregation;

        @Override
        public boolean equals( Object obj )
        {
            if (obj == null || !(obj instanceof ParameterKey))
            {
                return false;
            }

            ParameterKey otherKey = (ParameterKey)obj;

            boolean equal = StringUtils.equalsIgnoreCase( this.name, otherKey.name );
            equal = equal || StringUtils.equalsIgnoreCase( this.measurementUnit, otherKey.measurementUnit );
            equal = equal || StringUtils.equalsIgnoreCase( this.aggregation, otherKey.aggregation );

            return equal;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(name.toLowerCase(),
                                measurementUnit.toLowerCase(),
                                aggregation.toLowerCase());
        }

        @Override
        public int compareTo( ParameterKey parameterKey )
        {
            if (parameterKey == null)
            {
                return 1;
            }

            return Integer.compare( this.hashCode(), parameterKey.hashCode() );
        }
    }

    public static class USGSParameter
    {
        public USGSParameter( String line)
        {
            String[] lineParts = line.split("\\|");

            this.description = lineParts[0].replaceAll( "\"", "" );
            this.parameterCode = lineParts[1].replaceAll( "\"", "" );
            this.name = lineParts[2].replaceAll( "\"", "" );
            this.measurementUnit = lineParts[3].replaceAll("\"", "");
            this.aggregation = lineParts[4].replaceAll("\"", "");
        }

        public USGSParameter (ResultSet results) throws SQLException
        {
            this.name = results.getString("name");
            this.description = results.getString( "description" );
            this.parameterCode = results.getString("parameter_code");
            this.measurementUnit = results.getString("measurement_unit");
            this.aggregation = results.getString("aggregation");
            this.measurementUnitID = results.getInt( "measurementunit_id" );
        }

        private USGSParameter (
                final String name,
                final String description,
                final String parameterCode,
                final String aggregation,
                final String measurementUnit,
                final Integer measurementUnitID
        )
        {
            this.name = name;
            this.description = description;
            this.parameterCode = parameterCode;
            this.aggregation = aggregation;
            this.measurementUnit = measurementUnit;
            this.measurementUnitID = measurementUnitID;
        }

        @Override
        public String toString()
        {
            String parameter = "Name: '" + this.name + "', " +
                               "Description: '" + this.description + "', " +
                               "Code: " + this.parameterCode + ", " +
                               "Aggregated as: " + this.aggregation + ", " +
                               "Measurement Unit: " + this.measurementUnit;

            return parameter;
        }

        public String getName()
        {
            return name;
        }

        public String getDescription()
        {
            return description;
        }

        public String getParameterCode()
        {
            return parameterCode;
        }

        public String getAggregation()
        {
            return aggregation;
        }

        public String getMeasurementUnit()
        {
            return measurementUnit;
        }

        public Integer getMeasurementUnitID()
        {
            return measurementUnitID;
        }

        public ParameterKey getKey()
        {
            return new ParameterKey( this.getName(),
                                     this.getMeasurementUnit(),
                                     this.getAggregation() );
        }

        private String name;
        private String description;
        private String parameterCode;
        private String aggregation;
        private String measurementUnit;
        private Integer measurementUnitID;
    }

    private static final Object PARAMETER_LOCK = new Object();

    private static ConcurrentMap<ParameterKey, USGSParameter> parameterStore;

    private static ConcurrentMap<ParameterKey, USGSParameter> getParameterStore()
            throws SQLException
    {
        synchronized ( PARAMETER_LOCK )
        {
            if (USGSParameters.parameterStore == null)
            {
                USGSParameters.parameterStore = new ConcurrentSkipListMap<>(  );
                USGSParameters.init();
            }

            return USGSParameters.parameterStore;
        }
    }

    private static void init() throws SQLException
    {
        ScriptBuilder script = new ScriptBuilder(  );
        script.setHighPriority( true );

        script.add("SELECT * FROM wres.USGSParameter;");
        script.consume( parameter -> {
            USGSParameter usgsParameter = new USGSParameter( parameter );
            USGSParameters.parameterStore.putIfAbsent( usgsParameter.getKey(), usgsParameter );
        } );
    }

    public static USGSParameter getParameterByCode(final String code)
            throws SQLException
    {
        USGSParameter foundParameter = null;

        for (USGSParameter parameter : USGSParameters.getParameterStore().values())
        {
            if (parameter.getParameterCode().equals(code))
            {
                foundParameter = parameter;
                break;
            }
        }

        return foundParameter;
    }

    public static USGSParameter getParameter(String parameterName, String measurementUnit)
            throws SQLException
    {
        USGSParameter parameter = null;

        for (USGSParameters.ParameterKey key : USGSParameters.getParameterStore().keySet())
        {
            if (key.name.equalsIgnoreCase( parameterName ) &&
                key.measurementUnit.equalsIgnoreCase( measurementUnit ))
            {
                parameter = USGSParameters.getParameterStore().get(key);
                break;
            }
        }

        return parameter;
    }

    public static USGSParameter getParameter(String parameterName, String measurementUnit, String aggregationMethod)
            throws SQLException
    {
        USGSParameter parameter = null;

        ParameterKey key = new ParameterKey( parameterName, measurementUnit, aggregationMethod );

        if (USGSParameters.getParameterStore().containsKey( key ))
        {
            parameter = USGSParameters.getParameterStore().get( key );
        }
        else
        {
            String message = "There is not a known USGS parameter with the name '" +
                             parameterName +
                             "' and a measurement unit of " +
                             measurementUnit;

            if (aggregationMethod.equalsIgnoreCase( "none" ))
            {
                message += " that is not aggregated.";
            }
            else
            {
                message += " that is aggregated by " + aggregationMethod;
            }

            throw new IllegalArgumentException( message );

        }

        return parameter;
    }

    public static USGSParameter getParameterByDescription(String description)
            throws SQLException
    {
        USGSParameter matchingParameter = null;

        for (USGSParameter parameter : USGSParameters.getParameterStore().values())
        {
            if (parameter.getDescription().equalsIgnoreCase( description ))
            {
                matchingParameter = parameter;
                break;
            }
        }

        return matchingParameter;
    }

    public static USGSParameter addRequestedParameter(
            final String name,
            final String code,
            final String description,
            final String measurementUnit)
            throws SQLException
    {
        synchronized ( USGSParameters.PARAMETER_LOCK )
        {
            String usgsName = name.split( "," )[0];
            int measurementUnitId =
                    MeasurementUnits.getMeasurementUnitID( measurementUnit );

            ScriptBuilder script = new ScriptBuilder();
            script.addLine( "INSERT INTO wres.USGSParameter(" );
            script.addTab().addLine( "name," );
            script.addTab().addLine( "description," );
            script.addTab().addLine( "parameter_code," );
            script.addTab().addLine( "measurement_unit," );
            script.addTab().addLine( "measurementunit_id" );
            script.addLine( ")" );
            script.addLine( "VALUES (" );
            script.addTab().addLine( "'", usgsName, "'," );
            script.addTab().addLine( "'", description, "'," );
            script.addTab().addLine( "'", code, "'," );
            script.addTab().addLine( "'", measurementUnitId, "'," );
            script.addTab().addLine( "'None'" );
            script.addLine( ")" );
            script.add( "RETURNING *;" );

            List<USGSParameter> newParameter =
                    script.interpret( USGSParameter::new );

            USGSParameter parameter = null;

            if ( !newParameter.isEmpty() )
            {
                parameter = newParameter.get( 0 );
                USGSParameters.getParameterStore()
                              .putIfAbsent( parameter.getKey(), parameter );
            }

            return parameter;
        }
    }
}
