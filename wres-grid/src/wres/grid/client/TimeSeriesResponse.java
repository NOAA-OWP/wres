package wres.grid.client;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import wres.config.FeaturePlus;

class TimeSeriesResponse implements Response
{
    @Override
    public String getMeasurementUnit()
    {
        return this.measurementUnit;
    }

    @Override
    public String getVariableName()
    {
        return this.variableName;
    }

    static class TimeSeries implements Response.Series
    {
        TimeSeries( final FeaturePlus feature, final Instant issuedDate)
        {
            Objects.requireNonNull( feature, "No feature was supplied for this new TimeSeries" );
            Objects.requireNonNull( issuedDate,
                                    "No issued date was supplied for this new TimeSeries" );
            this.issuedDate = issuedDate;
            this.feature = feature;
            this.entries = new TreeMap<>();
        }

        void add(final Instant validDate, final double value)
        {
            Objects.requireNonNull( validDate, "A valid date must be supplied in order to add a value" );
            Duration leadDuration = Duration.ofSeconds( validDate.getEpochSecond() - this.issuedDate.getEpochSecond() );
            TimeSeriesEntry updatedEntry = null;

            if (this.entries.containsKey( leadDuration ))
            {
                updatedEntry = ((TimeSeriesEntry)this.entries.get(leadDuration)).add( value );
            }
            else
            {
                updatedEntry = new TimeSeriesEntry( validDate, leadDuration, value );
            }

            this.entries.put(leadDuration, updatedEntry);
        }

        @Override
        public boolean equals( Object obj )
        {
            return obj instanceof TimeSeries &&
                   ( ( TimeSeries ) obj ).issuedDate.equals( this.issuedDate );
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(this.issuedDate);
        }

        public FeaturePlus getFeature()
        {
            return feature;
        }

        public Instant getIssuedDate()
        {
            return issuedDate;
        }

        private final Map<Duration, Response.Entry> entries;
        private final Instant issuedDate;
        private final FeaturePlus feature;

        @Override
        public int compareTo( Response.Series entries )
        {
            int equality = this.feature.compareTo( entries.getFeature() );

            if (equality == 0)
            {
                equality = this.issuedDate.compareTo( entries.getIssuedDate() );
            }
            return equality;
        }

        @Override
        public Iterator<Response.Entry> iterator()
        {
            return this.entries.values().iterator();
        }
    }

    static class TimeSeriesEntry implements Response.Entry
    {
        TimeSeriesEntry( final Instant validDate, final Duration lead, final Double value)
        {
            this.validDate = validDate;
            this.lead = lead;
            List<Double> newValues = new ArrayList<>();
            newValues.add( value );
            this.values = Collections.unmodifiableList( newValues );
        }

        TimeSeriesEntry(final Instant validDate, final Duration lead, final List<Double> values)
        {
            this.validDate = validDate;
            this.lead = lead;
            this.values = Collections.unmodifiableList( values );
        }

        TimeSeriesEntry add(Double value)
        {
            List<Double> newValues = new ArrayList<>(this.values);
            newValues.add(value);
            return new TimeSeriesEntry( this.validDate, this.lead, newValues );
        }

        public Instant getValidDate()
        {
            return validDate;
        }

        public Duration getLead()
        {
            return lead;
        }

        public Double[] getMeasurements()
        {
            return values.toArray( new Double[this.values.size()] );
        }

        private final Instant validDate;
        private final Duration lead;
        private final List<Double> values;

        @Override
        public Iterator<Double> iterator()
        {
            return this.values.iterator();
        }

        @Override
        public int compareTo( Response.Entry doubles )
        {
            return this.lead.compareTo( doubles.getLead() );
        }
    }

    TimeSeriesResponse()
    {
        this.timeSeriesPerFeature = new TreeMap<>(  );
    }

    void setMeasurementUnit(String measurementUnit)
    {
        this.measurementUnit = measurementUnit;
    }

    void setVariableName(String variableName)
    {
        this.variableName = variableName;
    }

    void add(final FeaturePlus feature,
             final Instant issuedDate,
             final Instant validDate,
             final double value)
    {
        if (!this.timeSeriesPerFeature.containsKey( feature ))
        {
            this.timeSeriesPerFeature.put(feature, new ArrayList<>());
        }

        Series series = this.timeSeriesPerFeature.get(feature)
                                                     .stream()
                                                     .filter( timeSeries -> timeSeries.getIssuedDate().equals( issuedDate ) )
                                                     .findFirst()
                                                     .orElse( null );

        if (series == null)
        {
            series = new TimeSeries( feature, issuedDate );
            this.timeSeriesPerFeature.get(feature).add( series );
        }

        ((TimeSeries)series).add( validDate, value );
    }

    private final Map<FeaturePlus, List<Series>> timeSeriesPerFeature;
    private String measurementUnit;
    private String variableName;

    @Override
    public Iterator<List<Series>> iterator()
    {
        return this.timeSeriesPerFeature.values().iterator();
    }
}
