package wres.io.retrieval;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.TemporalAccessor;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

import wres.io.data.caching.UnitConversions;
import wres.io.project.Project;
import wres.io.utilities.DataProvider;

/**
 * TODO: replace all references to integer leads with {@link Duration}. This will unravel the integer arithmetic in 
 * {@link wres.io.retrieval.IngestedValueCollection}, but that needs to be unraveled.
 */

class IngestedValue implements Comparable<IngestedValue>
{
    private static final Object CONVERSION_LOCK = new Object();
    private static Map<Integer, UnitConversions.Conversion> conversionMap;

    private final long referenceEpoch;
    private final Instant validTime;
    private final int lead;
    private final Double[] measurements;
    private final Integer[] members;

    private static UnitConversions.Conversion getConversion(int measurementUnitID, String desiredUnit)
    {
        synchronized ( CONVERSION_LOCK )
        {
            if (IngestedValue.conversionMap == null)
            {
                IngestedValue.conversionMap = new TreeMap<>(  );
            }

            if (!IngestedValue.conversionMap.containsKey( measurementUnitID ))
            {
                IngestedValue.conversionMap.put(
                        measurementUnitID,
                        UnitConversions.getConversion( measurementUnitID, desiredUnit )
                );
            }

            return IngestedValue.conversionMap.get( measurementUnitID );
        }
    }

    IngestedValue( DataProvider row, Project project )
    {
        this.validTime = row.getInstant( "value_date" );
        this.measurements = row.getDoubleArray("measurements" );
        this.convertAllMeasurements(
                row.getInt( "measurementunit_id" ),
                project
        );
        this.members = row.getIntegerArray( "members" );
        this.lead = row.getInt( "lead" );
        this.referenceEpoch = row.getLong("basis_epoch_time");
    }

    IngestedValue(Instant validTime,
                  Double[] measurements,
                  int measurementUnitId,
                  int lead,
                  long basisEpoch,
                  Project project )
    {
        this.validTime = validTime;
        this.measurements = measurements;
        this.convertAllMeasurements( measurementUnitId, project );
        this.members = new Integer[measurements.length];
        // Each value is the default due to a lack of extra information
        // Low risk since gridded ensembles are not frequently used
        Arrays.fill( this.members, 1 );
        this.lead = lead;
        this.referenceEpoch = basisEpoch;
    }

    int getLead()
    {
        return this.lead;
    }

    int length()
    {
        return this.measurements.length;
    }

    Double get(int index)
    {
        Double value = null;

        if (index >= 0 && index < this.length())
        {
            value = this.measurements[index];
        }

        return value;
    }

    Integer getMemberID(int index)
    {
        Integer member = null;

        if (index >= 0 && index < this.length())
        {
            member = this.members[index];
        }

        return member;
    }

    Instant getValidTime()
    {
        return this.validTime;
    }

    long getReferenceEpoch()
    {
        return this.referenceEpoch;
    }

    private TemporalAccessor getReferenceTime()
    {
        return Instant.ofEpochSecond( this.referenceEpoch );
    }

    /**
     * Converts all stored values into the desired measurement unit
     * TODO: Find way to remove the need for a ProjectDetails object
     * @param measurementunitId The ID of the unit of measurement that all collected values are in
     * @param project The object storing information about how values should be handled
     */
    private void convertAllMeasurements(int measurementunitId, Project project )
    {
        for (int index = 0; index < this.length(); ++index)
        {
            Double value = this.convertMeasurement(
                    this.get( index ),
                    measurementunitId,
                    project.getDesiredMeasurementUnit()
            );

            if ( value < project.getMinimumValue())
            {
                value = project.getDefaultMinimumValue();
            }
            else if ( value > project.getMaximumValue())
            {
                value = project.getDefaultMaximumValue();
            }

            if (value == null)
            {
                value = Double.NaN;
            }

            this.measurements[index] = value;
        }
    }

    private Double convertMeasurement(Double measurement, int measurementunitId, String desiredUnit)
    {
        UnitConversions.Conversion conversion = IngestedValue.getConversion( measurementunitId, desiredUnit );
        Double convertedMeasurement = Double.NaN;

        if (measurement != null && !measurement.isNaN() && conversion != null)
        {
            convertedMeasurement = conversion.convert( measurement );
        }

        return convertedMeasurement;
    }

    @Override
    public int compareTo( IngestedValue ingestedValue )
    {
        return Integer.compare( this.getLead(), ingestedValue.getLead() );
    }

    @Override
    public String toString()
    {

        return "Lead: " + this.getLead() + ", " +
               "Valid Date: " + this.getValidTime() + ", " +
               "Measurements: " + Arrays.toString( this.measurements )
               +
               ", Reference Date: " + this.getReferenceTime();
    }
}
