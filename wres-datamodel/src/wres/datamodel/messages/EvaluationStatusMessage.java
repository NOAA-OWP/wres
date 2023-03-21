package wres.datamodel.messages;

import java.util.Objects;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import wres.statistics.generated.EvaluationStatus.EvaluationStatusEvent;
import wres.statistics.generated.EvaluationStatus.EvaluationStatusEvent.EvaluationStage;
import wres.statistics.generated.EvaluationStatus.EvaluationStatusEvent.StatusLevel;

/**
 * <p>Wraps an {@link EvaluationStatusEvent}, which contains user-facing information about the status of an evaluation.
 *
 * @author James Brown
 */

public class EvaluationStatusMessage implements Comparable<EvaluationStatusMessage>
{
    /** The canonical evaluation status. */
    private final EvaluationStatusEvent statusMessage;

    private static final String MESSAGE = "MESSAGE";
    private static final String STAGE = "STAGE";
    private static final String LEVEL = "LEVEL";

    /**
     * Constructs an event.
     *
     * @param statusEvent the evaluation status event, not null
     * @throws NullPointerException if the statusEvent is null
     * @return an evaluation status message
     */

    public static EvaluationStatusMessage of( EvaluationStatusEvent statusEvent )
    {
        return new EvaluationStatusMessage( statusEvent );
    }

    /**
     * Constructs an event.
     *
     * @param statusLevel the status level
     * @param evaluationStage the evaluation stage
     * @param message the message
     * @throws NullPointerException if any input is null
     * @return an evaluation status message
     */

    public static EvaluationStatusMessage of( StatusLevel statusLevel, EvaluationStage evaluationStage, String message )
    {
        return EvaluationStatusMessage.getEveluationStatusEvent( statusLevel, evaluationStage, message );
    }

    /**
     * Constructs an event of type {@link StatusLevel#WARN}.
     *
     * @param evaluationStage the evaluation stage
     * @param message the message
     * @throws NullPointerException if any input is null
     * @return an evaluation status message
     */

    public static EvaluationStatusMessage warn( EvaluationStage evaluationStage, String message )
    {
        return EvaluationStatusMessage.getEveluationStatusEvent( StatusLevel.WARN, evaluationStage, message );
    }

    /**
     * Constructs an event of type {@link StatusLevel#ERROR}.
     *
     * @param evaluationStage the evaluation stage
     * @param message the message
     * @throws NullPointerException if any input is null
     * @return an evaluation status message
     */

    public static EvaluationStatusMessage error( EvaluationStage evaluationStage, String message )
    {
        return EvaluationStatusMessage.getEveluationStatusEvent( StatusLevel.ERROR, evaluationStage, message );
    }

    /**
     * Constructs an event of type {@link StatusLevel#ERROR}.
     *
     * @param evaluationStage the evaluation stage
     * @param message the message
     * @throws NullPointerException if any input is null
     * @return an evaluation status message
     */

    public static EvaluationStatusMessage info( EvaluationStage evaluationStage, String message )
    {
        return EvaluationStatusMessage.getEveluationStatusEvent( StatusLevel.INFO, evaluationStage, message );
    }

    /**
     * Constructs an event of type {@link StatusLevel#DEBUG}.
     *
     * @param evaluationStage the evaluation stage
     * @param message the message
     * @throws NullPointerException if any input is null
     * @return an evaluation status message
     */

    public static EvaluationStatusMessage debug( EvaluationStage evaluationStage, String message )
    {
        return EvaluationStatusMessage.getEveluationStatusEvent( StatusLevel.DEBUG, evaluationStage, message );
    }

    /**
     * Returns the status level.
     *
     * @return the status level
     */

    public StatusLevel getStatusLevel()
    {
        return this.statusMessage.getStatusLevel();
    }

    /**
     * Returns the evaluation stage.
     *
     * @return the evaluation stage
     */

    public EvaluationStage getEvaluationStage()
    {
        return this.statusMessage.getEvaluationStage();
    }

    /**
     * Returns the canonical status message.
     *
     * @return the event type
     */

    public EvaluationStatusEvent getEvaluationStatusEvent()
    {
        return this.statusMessage;
    }

    /**
     * Returns the message.
     *
     * @return the validation message
     */

    public String getMessage()
    {
        return this.statusMessage.getEventMessage();
    }

    /**
     * Provides a string representation of the validation event.
     *
     * @return a string representation
     */

    @Override
    public String toString()
    {
        return new ToStringBuilder( this, ToStringStyle.SHORT_PREFIX_STYLE )
                .append( LEVEL, this.getStatusLevel() )
                .append( STAGE,
                         this.getEvaluationStage() )
                .append( MESSAGE, this.getMessage() )
                .toString();
    }

    @Override
    public int compareTo( EvaluationStatusMessage o )
    {
        return MessageUtilities.compare( this.getEvaluationStatusEvent(), o.getEvaluationStatusEvent() );
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( !( obj instanceof EvaluationStatusMessage input ) )
        {
            return false;
        }

        return Objects.equals( input.getEvaluationStatusEvent(), this.getEvaluationStatusEvent() );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( this.getEvaluationStatusEvent() );
    }

    /**
     * @param statusLevel the status level, not null
     * @param evaluationStage the evaluation stage, not null
     * @param message the message string, not null
     * @return the message
     * @throws NullPointerException if either input is null
     */

    private static EvaluationStatusMessage getEveluationStatusEvent( StatusLevel statusLevel,
                                                                     EvaluationStage evaluationStage,
                                                                     String message )
    {
        Objects.requireNonNull( statusLevel );
        Objects.requireNonNull( evaluationStage );
        Objects.requireNonNull( message );

        EvaluationStatusEvent event = EvaluationStatusEvent.newBuilder()
                                                           .setStatusLevel( statusLevel )
                                                           .setEventMessage( message )
                                                           .setEvaluationStage( evaluationStage )
                                                           .build();

        return new EvaluationStatusMessage( event );
    }

    /**
     * Hidden constructor.
     *
     * @param statusMessage the canonical status message
     * @throws NullPointerException if the statusMessage is null
     */

    private EvaluationStatusMessage( EvaluationStatusEvent statusMessage )
    {
        Objects.requireNonNull( statusMessage, "Specify a non-null evaluation status message." );

        this.statusMessage = statusMessage;
    }

}
