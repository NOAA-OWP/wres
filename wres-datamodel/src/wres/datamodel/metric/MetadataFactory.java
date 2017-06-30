package wres.datamodel.metric;

import java.util.Objects;

/**
 * A factory class for constructing {@link Metadata} and associated objects.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public final class MetadataFactory
{

    /**
     * Build a {@link Metadata} object with a sample size and a default {@link Dimension}.
     * 
     * @param sampleSize the sample size
     * @return a {@link Metadata} object
     */

    public static Metadata getMetadata(final int sampleSize)
    {
        return getMetadata(sampleSize, getDimension(), null);
    }

    /**
     * Build a {@link Metadata} object with a sample size and a prescribed {@link Dimension} and identifiers for the
     * input data and, possible, a baseline (may be null).
     * 
     * @param sampleSize the sample size
     * @param dim the dimension
     * @param id an identifier associated with the metric data (may be null)
     * @return a {@link Metadata} object
     */

    public static Metadata getMetadata(final int sampleSize,
                                       final Dimension dim,
                                       final String id)
    {
        return new MetadataImpl(sampleSize, dim, id);
    }

    /**
     * Builds a default {@link MetricOutputMetadata} with a prescribed sample size, {@link Dimension}, and identifiers
     * for the metric and the metric component, as well as the data and baseline data (may be null).
     * 
     * @param sampleSize the sample size
     * @param dim the dimension
     * @param metricID the metric identifier
     * @param mainID an identifier associated with the metric data (may be null)
     * @param baseID an identifier associated with the baseline metric data (may be null)
     * @param componentID the metric component identifier
     * @return a {@link MetricOutputMetadata} object
     */

    public static MetricOutputMetadata getMetadata(final int sampleSize,
                                                   final Dimension dim,
                                                   final int metricID,
                                                   final int componentID,
                                                   final String mainID,
                                                   final String baseID)
    {
        class MetricOutputMetadataImpl extends MetadataImpl implements MetricOutputMetadata
        {

            private MetricOutputMetadataImpl()
            {
                super(sampleSize, dim, mainID);
            }

            @Override
            public int getMetricID()
            {
                return metricID;
            }

            @Override
            public int getMetricComponentID()
            {
                return componentID;
            }

            @Override
            public String getIDForBaseline()
            {
                return baseID;
            }

            @Override
            public boolean equals(final Object o)
            {
                boolean returnMe = super.equals(o) && o instanceof MetricOutputMetadata
                    && ((MetricOutputMetadata)o).getMetricID() == getMetricID()
                    && ((MetricOutputMetadata)o).getMetricComponentID() == getMetricComponentID()
                    && Objects.isNull(((MetricOutputMetadata)o).getIDForBaseline()) == Objects.isNull(getIDForBaseline());
                if(!Objects.isNull(getID()))
                {
                    returnMe = returnMe && getID().equals(((Metadata)o).getID());
                }
                if(!Objects.isNull(getIDForBaseline()))
                {
                    returnMe = returnMe && getIDForBaseline().equals(((MetricOutputMetadata)o).getIDForBaseline());
                }
                return returnMe;
            }

            @Override
            public int hashCode()
            {
                int returnMe = super.hashCode() + Integer.hashCode(metricID) + Integer.hashCode(componentID)
                    + Boolean.hashCode(Objects.isNull(getIDForBaseline()));
                if(!Objects.isNull(getIDForBaseline()))
                {
                    returnMe += getIDForBaseline().hashCode();
                }
                return returnMe;
            }

            @Override
            public String toString()
            {
                final StringBuilder b = new StringBuilder();
                b.append("[")
                 .append(sampleSize)
                 .append(",")
                 .append(dim)
                 .append(",")
                 .append(metricID)
                 .append(",")
                 .append(componentID)
                 .append(",")
                 .append(mainID)
                 .append(",")
                 .append(baseID)
                 .append("].");
                return b.toString();
            }

        }
        return new MetricOutputMetadataImpl();
    }

    /**
     * Returns a {@link Dimension} that is nominally dimensionless.
     * 
     * @return a {@link Dimension}
     */

    public static Dimension getDimension()
    {
        return getDimension("DIMENSIONLESS");
    }

    /**
     * Returns a {@link Dimension} with a named dimension and {@link Dimension#hasDimension()} that returns false if the
     * dimension is "DIMENSIONLESS", true otherwise.
     * 
     * @param dimension the dimension string
     * @return a {@link Dimension}
     */

    public static Dimension getDimension(final String dimension)
    {
        class DimensionImpl implements Dimension
        {
            /**
             * The dimension.
             */
            private final String dimension;

            public DimensionImpl(final String dimension)
            {
                Objects.requireNonNull("Specify a non-null dimension string.", dimension);
                this.dimension = dimension;
            }

            @Override
            public boolean hasDimension()
            {
                return dimension.equals("DIMENSIONLESS");
            }

            @Override
            public String getDimension()
            {
                return dimension;
            }

            @Override
            public boolean equals(final Object o)
            {
                return o instanceof Dimension && ((Dimension)o).hasDimension() == hasDimension()
                    && ((Dimension)o).getDimension().equals(getDimension());
            }

            @Override
            public int hashCode()
            {
                return Boolean.hashCode(hasDimension());
            }

            @Override
            public String toString()
            {
                return getDimension();
            }
        }
        return new DimensionImpl(dimension);
    }

    /**
     * Basic implementation of metadata.
     */

    private static class MetadataImpl implements Metadata
    {
        private final int sampleSize;
        private final Dimension dim;
        private final String mainID;

        private MetadataImpl(final int sampleSize, final Dimension dim, final String mainID)
        {
            this.sampleSize = sampleSize;
            this.dim = dim;
            this.mainID = mainID;
        }

        @Override
        public int getSampleSize()
        {
            return sampleSize;
        }

        @Override
        public Dimension getDimension()
        {
            return dim;
        }

        @Override
        public String getID()
        {
            return mainID;
        }

        @Override
        public boolean equals(final Object o)
        {
            boolean returnMe = o instanceof Metadata && ((Metadata)o).getSampleSize() == getSampleSize()
                && ((Metadata)o).getDimension().equals(getDimension())
                && Objects.isNull(((Metadata)o).getID()) == Objects.isNull(getID());
            if(!Objects.isNull(getID()))
            {
                returnMe = returnMe && getID().equals(((Metadata)o).getID());
            }
            return returnMe;
        }

        @Override
        public int hashCode()
        {
            int returnMe = Integer.hashCode(sampleSize) + getDimension().hashCode()
                + Boolean.hashCode(Objects.isNull(getID()));
            if(!Objects.isNull(getID()))
            {
                returnMe += getID().hashCode();
            }
            return returnMe;
        }

        @Override
        public String toString()
        {
            final StringBuilder b = new StringBuilder();
            b.append("[")
             .append(sampleSize)
             .append(",")
             .append(dim)
             .append(",")
             .append(mainID)
             .append("].");
            return b.toString();
        }
    }

    /**
     * No argument constructor.
     */

    private MetadataFactory()
    {
    };

}
