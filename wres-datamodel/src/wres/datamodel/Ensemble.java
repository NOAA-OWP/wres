package wres.datamodel;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;

/**
 * Stores an array of ensemble member values as double values and, optionally, an array of ensemble member labels.
 * 
 * @author james.brown@hydrosolved.com
 */
public class Ensemble implements Comparable<Ensemble>
{
    
    /**
     * The ensemble members.
     */

    private final double[] members;

    /**
     * The labels, which may be null.
     */

    private final String[] labels;

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
     * members are ordered according to the array-ordering of the labels.
     * 
     * @param members the ensemble members
     * @param labels the labels
     * @return the ensemble
     * @throws IllegalArgumentException if the two inputs have different size
     */

    public static Ensemble of( final double[] members, String[] labels )
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
        
        for( int i = 0; i < labels.length; i++ )
        {
            if( label.equals( labels[i] ) )
            {
                return members[i];
            }
        }
        
        throw new IllegalArgumentException( "Unrecognized label '"+label+"'." );
    }
    
    /**
     * Returns a copy of the optional labels.
     * 
     * @return the labels
     */

    public Optional<String[]> getLabels()
    {
        if( Objects.isNull( labels ) )
        {
            return Optional.empty();
        }
        
        return Optional.of( labels.clone() );
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

        returnMe = Boolean.compare( this.getLabels().isPresent(), other.getLabels().isPresent() );

        if ( returnMe != 0 )
        {
            return returnMe;
        }

        if( this.getLabels().isPresent() )
        {
            Comparator<String[]> compare = Comparator.nullsFirst( ( a, b ) -> Arrays.compare( a, b ) );

            return Objects.compare( this.getLabels().get(), other.getLabels().get(), compare );
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
        if ( this.getLabels().isPresent() != otherVec.getLabels().isPresent() )
        {
            return false;
        }

        // Labels
        if ( this.getLabels().isPresent() )
        {
            return Arrays.equals( this.getMembers(), otherVec.getMembers() )
                   && Arrays.equals( this.getLabels().get(), otherVec.getLabels().get() );
        }

        // No labels
        return Arrays.equals( this.getMembers(), otherVec.getMembers() );
    }

    @Override
    public int hashCode()
    {
        // Labels?
        if ( this.getLabels().isPresent() )
        {
            return Objects.hash( Arrays.hashCode( this.getMembers() ), Arrays.hashCode( this.getLabels().get() ) );
        }

        return Arrays.hashCode( this.getMembers() );
    }

    @Override
    public String toString()
    {
        if ( this.getLabels().isPresent() )
        {
            StringJoiner joiner = new StringJoiner( ",", "[", "]" );

            for ( int i = 0; i < members.length; i++ )
            {
                joiner.add( "{" + labels[i] + "," + members[i] + "}" );
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
        this( members.clone(), null );
    }

    /**
     * Hidden constructor.
     * 
     * @param members the ensemble members
     * @param labels the ensemble member labels
     * @throws IllegalArgumentException if the labels are non-null and differ in size to the number of members
     */

    private Ensemble( final double[] members, String[] labels )
    {
        Objects.requireNonNull( members );

        if ( Objects.nonNull( labels ) )
        {
            if ( members.length != labels.length )
            {
                throw new IllegalArgumentException( "Expected the same number of members (" + members.length
                                                    + ") as labels ("
                                                    + labels.length
                                                    + ")." );
            }

            this.labels = labels.clone();
        }
        else
        {
            this.labels = null;
        }

        this.members = members.clone();
    }

}
