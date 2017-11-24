package wres.io.data.caching;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;

import wres.io.utilities.Database;
import wres.util.Strings;

public class USGSParameters
{
    private static class ParameterKey implements Comparable<ParameterKey>
    {
        public ParameterKey(String name, String measurementUnit, String aggregation)
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
            if (obj == null || !(obj  instanceof ParameterKey))
            {
                return false;
            }

            boolean equal;

            ParameterKey otherKey = (ParameterKey)obj;

            if (this.name != null && otherKey.name != null)
            {
                equal = this.name.equalsIgnoreCase( otherKey.name );
            }
            else
            {
                equal = this.name == null && otherKey.name == null;
            }

            if (equal)
            {
                if (this.measurementUnit != null && otherKey.measurementUnit != null)
                {
                    equal = this.measurementUnit.equalsIgnoreCase( otherKey.measurementUnit );
                }
                else
                {
                    equal = this.measurementUnit == null && otherKey.measurementUnit == null;
                }
            }

            if (equal)
            {
                if (this.aggregation != null && otherKey.aggregation != null)
                {
                    equal = this.aggregation.equalsIgnoreCase( otherKey.aggregation );
                }
                else
                {
                    equal = this.aggregation == null && otherKey.aggregation == null;
                }
            }

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
        private static final String DESCRIPTION_PATTERN = "(?=\").+(?<=\",)";

        public USGSParameter( String line)
        {
            this.description = Strings.extractWord( line, DESCRIPTION_PATTERN);
            this.description = this.description.replaceAll( "\",", "" );
            this.description = this.description.replaceAll( "\"", "" );
            line = line.replaceAll( DESCRIPTION_PATTERN, "" );
            String[] lineParts = line.split(",");
            this.parameterCode = lineParts[0];
            this.name = lineParts[1];
            this.measurementUnit = lineParts[2];
            this.aggregation = lineParts[3];
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

        @Override
        public String toString()
        {
            StringBuilder parameter = new StringBuilder(  );

            parameter.append( "Name: '" ).append(this.name).append("', ");
            parameter.append( "Description: '").append(this.description).append("', ");
            parameter.append( "Code: " ).append(this.parameterCode).append(", ");
            parameter.append( "Aggregated as: ").append(this.aggregation).append(", ");
            parameter.append( "Measurement Unit: ").append(this.measurementUnit);

            return parameter.toString();
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
        Connection connection = null;
        ResultSet parameters = null;

        try
        {
            connection = Database.getHighPriorityConnection();
            parameters = Database.getResults( connection, "SELECT * FROM wres.USGSParameter;" );

            while (parameters.next())
            {
                USGSParameter parameter = new USGSParameter( parameters );
                USGSParameters.parameterStore.putIfAbsent( parameter.getKey(), parameter );
            }
        }
        finally
        {
            if (connection != null)
            {
                Database.returnHighPriorityConnection( connection );
            }
        }
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
                             String.valueOf(parameterName) +
                             "' and a measurement unit of " +
                             String.valueOf(measurementUnit);

            if (aggregationMethod.equalsIgnoreCase( "none" ))
            {
                message += " that is not aggregated.";
            }
            else
            {
                message += " that is aggregated by " + String.valueOf(aggregationMethod);
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

    public static String getCode(String parameterName, String measurementUnit, String aggregationMethod)
            throws SQLException
    {
        String code;
        ParameterKey key = new ParameterKey( parameterName, measurementUnit, aggregationMethod );

        if (USGSParameters.getParameterStore().containsKey( key ))
        {
            code = USGSParameters.getParameterStore().get( key ).getParameterCode();
        }
        else
        {
            String message = "There is not a known USGS parameter with the name '" +
                             String.valueOf(parameterName) +
                             "' and a measurement unit of " +
                             String.valueOf(measurementUnit);

            if (aggregationMethod.equalsIgnoreCase( "none" ))
            {
                message += " that is not aggregated.";
            }
            else
            {
                message += " that is aggregated by " + String.valueOf(aggregationMethod);
            }

            throw new IllegalArgumentException( message );

        }

        return code;
    }
}
