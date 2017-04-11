/**
 * 
 */
package util.exceptions;

/**
 * @author ctubbs
 *
 */
public class InCompleteException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6789320525702238392L;

	/**
	 * 
	 */
	public InCompleteException() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param message
	 */
	public InCompleteException(String message) {
		super(message);
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param cause
	 */
	public InCompleteException(Throwable cause) {
		super(cause);
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param message
	 * @param cause
	 */
	public InCompleteException(String message, Throwable cause) {
		super(message, cause);
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param message
	 * @param cause
	 * @param enableSuppression
	 * @param writableStackTrace
	 */
	public InCompleteException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
		// TODO Auto-generated constructor stub
	}

}
