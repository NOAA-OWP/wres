package wres.io.retrieval;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.temporal.TemporalAccessor;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

import wres.io.data.caching.UnitConversions;
import wres.io.data.details.ProjectDetails;
import wres.io.utilities.Database;

class IngestedValue implements Comparable<IngestedValue>
{
    private static final Object CONVERSION_LOCK = new Object();
    private static Map<Integer, UnitConversions.Conversion> conversionMap;

    private final long referenceEpoch;
    private final Instant validTime;
    private final int lead;
    private final Double[] measurements;

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

    IngestedValue( ResultSet row, ProjectDetails projectDetails) throws SQLException
    {
        this.validTime = Database.getInstant(row, "value_date");
        this.measurements = (Double[])row.getArray( "measurements" ).getArray();
        this.convertAllMeasurements(
                Database.getValue( row, "measurementunit_id" ),
                projectDetails
        );

        this.lead = Database.getValue( row, "lead" );
        this.referenceEpoch = Database.getValue( row, "basis_epoch_time" );
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

    private void convertAllMeasurements(int measurementunitId, ProjectDetails projectDetails)
    {
        for (int index = 0; index < this.length(); ++index)
        {
            Double value = this.convertMeasurement(
                    this.get( index ),
                    measurementunitId,
                    projectDetails.getDesiredMeasurementUnit()
            );

            if (value < projectDetails.getMinimumValue())
            {
                value = projectDetails.getDefaultMinimumValue();
            }
            else if (value > projectDetails.getMaximumValue())
            {
                value = projectDetails.getDefaultMaximumValue();
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
        String builder = "Lead: " + this.getLead() + ", " +
                         "Valid Date: " + this.getValidTime() + ", " +
                         "Measurements: " + Arrays.toString( this.measurements )
                         +
                         ", Reference Date: " + this.getReferenceTime();

        return builder;
    }
}
