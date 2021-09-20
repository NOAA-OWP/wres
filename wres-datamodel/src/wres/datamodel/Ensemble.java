package wres.datamodel;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;
import java.util.StringJoiner;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

/**
 * Stores an array of ensemble member values as double values and, optionally, an array of ensemble member labels.
 * 
 * @author james.brown@hydrosolved.com
 */
public class Ensemble implements Comparable<Ensemble>
{

    /**
     * Ensemble labels.
     */

    public static class Labels
    {

        /**
         * A cache of ensemble names to re-use, indexed by the concatenation of the names as a unique identifier. Allow 
         * one hundred; there should not be more than a handful of instances across recent evaluations. 
         */

        private static final Cache<String, Labels> LABELS_CACHE = Caffeine.newBuilder()
                                                                          .maximumSize( 100 )
                                                                          .build();

        /**
         * Empty labels.
         */

        private static final Labels EMPTY_LABELS = new Labels( new String[0] );

        /**
         * The labels, which may be empty.
         */

        private final String[] labs;

        /**
         * @param labels the labels
         * @return an instance for the input labels
         */

        public static Labels of( String... labels )
        {
            return Labels.getFromCache( labels );
        }

        /**
         * @return an instance with no labels defined
         */

        public static Labels of()
        {
            return Labels.EMPTY_LABELS;
        }

        /**
         * @return a clone of the labels.
         */

        public String[] getLabels()
        {
            return this.labs.clone();
        }

        /**
         * @return {@code true} if one or more labels is defined, {@code false} if no labels are defined.
         */

        public boolean hasLabels()
        {
            return this.labs.length > 0;
        }

        /**
         * @param ensembleName the ensemble name
         * @return {@code true} if the prescribed label is present, otherwise {@code false}.
         * @throws NullPointerException if the ensembleName is null.
         */

        public boolean hasLabel( String ensembleName )
        {
            Objects.requireNonNull( ensembleName );

            // Labels are de-duplicated, so search the unordered labels, else would need to store de-duplicated ordered
            // labels to use a search that relies on order
            for ( String next : labs )
            {
                if ( ensembleName.equals( next ) )
                {
                    return true;
                }
            }

            return false;
        }

        @Override
        public boolean equals( Object other )
        {
            if ( other == this )
            {
                return true;
            }

            if ( ! ( other instanceof Labels ) )
            {
                return false;
            }

            Labels otherLabels = (Labels) other;

            return Arrays.equals( this.labs, otherLabels.labs );
        }

        @Override
        public int hashCode()
        {
            return Arrays.hashCode( this.labs );
        }

        /**
         * @return a set of labels from the cache
         */

        private static Labels getFromCache( String[] labels )
        {
            if ( Objects.isNull( labels ) || labels.length == 0 )
            {
                return Labels.EMPTY_LABELS;
            }

            String id = String.join( ",", labels );

            Labels cached = Labels.LABELS_CACHE.getIfPresent( id );

            if ( Objects.nonNull( cached ) )
            {
                return cached;
            }

            // Add to cache
            cached = new Labels( labels );
            Labels.LABELS_CACHE.put( id, cached );

            return cached;
        }

        /**
         * Hidden constructor
         * @param labels the labels
         */

        private Labels( String[] labels )
        {
            Objects.requireNonNull( labels );

            this.labs = labels.clone();
        }
    }

    /**
     * The ensemble members.
     */

    private final double[] members;

    /**
     * The member labels.
     */

    private final Labels labels;

    /**
     * Returns a {@link Ensemble} from a primitive array of members.
     * 
     * @param members the ensemble members
     * @return the ensemble
     */

    public static Ensemble of( final double... members )
    {
        return new Ensemble( members );
    }

    /**
     * Returns a {@link Ensemble} from a primitive array of members and a corresponding array of member labels. The
     * members are ordered according to the order of the labels.
     * 
     * @param members the ensemble members
     * @param labels the labels
     * @return the ensemble
     * @throws IllegalArgumentException if the two inputs have different size
     */

    public static Ensemble of( final double[] members, Labels labels )
    {
        return new Ensemble( members, labels );
    }

    /**
     * Returns a copy of the ensemble members.
     * 
     * @return the members
     */

    public double[] getMembers()
    {
        return members.clone();
    }

    /**
     * Returns a member that corresponds to a prescribed label.
     * 
     * @param label the label
     * @return the ensemble member
     * @throws IllegalArgumentException if the label is not present
     * @throws NullPointerException if the input is null
     */

    public double getMember( String label )
    {
        Objects.requireNonNull( label );

        for ( int i = 0; i < this.labels.labs.length; i++ )
        {
            if ( label.equals( this.labels.labs[i] ) )
            {
                return this.members[i];
            }
        }

        throw new IllegalArgumentException( "Unrecognized label '" + label + "'." );
    }

    /**
     * Returns the labels.
     * 
     * @return the labels
     */

    public Labels getLabels()
    {
        return this.labels;
    }

    /**
     * Return the number of members present.
     * 
     * @return the number of members
     */

    public int size()
    {
        return members.length;
    }

    @Override
    public int compareTo( Ensemble other )
    {
        Objects.requireNonNull( other );

        int returnMe = Arrays.compare( this.getMembers(), other.getMembers() );

        if ( returnMe != 0 )
        {
            return returnMe;
        }

        Labels labs = this.getLabels();
        Labels otherLabs = other.getLabels();

        returnMe = Boolean.compare( labs.hasLabels(), otherLabs.hasLabels() );

        if ( returnMe != 0 )
        {
            return returnMe;
        }

        if ( labs.hasLabels() )
        {
            Comparator<String[]> compare = Comparator.nullsFirst( Arrays::compare );

            return Objects.compare( labs.labs, otherLabs.labs, compare );
        }

        return 0;
    }

    @Override
    public boolean equals( Object other )
    {
        if ( ! ( other instanceof Ensemble ) )
        {
            return false;
        }

        Ensemble otherVec = (Ensemble) other;

        // Label status?
        if ( !this.getLabels().equals( otherVec.getLabels() ) )
        {
            return false;
        }

        return Arrays.equals( this.getMembers(), otherVec.getMembers() );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( this.labels, Arrays.hashCode( this.getMembers() ) );
    }

    @Override
    public String toString()
    {
        if ( this.getLabels().hasLabels() )
        {
            StringJoiner joiner = new StringJoiner( ",", "[", "]" );

            for ( int i = 0; i < this.members.length; i++ )
            {
                joiner.add( "{" + this.labels.labs[i] + "," + this.members[i] + "}" );
            }

            return joiner.toString();
        }
        else
        {
            return Arrays.toString( this.getMembers() ).replace( " ", "" );
        }
    }

    /**
     * Hidden constructor.
     * 
     * @param members the ensemble members
     */

    private Ensemble( final double[] members )
    {
        this( members.clone(), Labels.EMPTY_LABELS );
    }

    /**
     * Hidden constructor.
     * 
     * @param members the ensemble members
     * @param labels the ensemble member labels
     * @throws IllegalArgumentException if the labels are non-null and differ in size to the number of members
     * @throws NullPointerException if the labels are null
     */

    private Ensemble( final double[] members, Labels labels )
    {
        Objects.requireNonNull( members );
        Objects.requireNonNull( labels );

        if ( labels.hasLabels() && members.length != labels.labs.length )
        {
            throw new IllegalArgumentException( "Expected the same number of members (" + members.length
                                                + ") as labels ("
                                                + labels.labs.length
                                                + ")." );
        }

        this.labels = labels;
        this.members = members.clone();
    }

}
