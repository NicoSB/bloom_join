package com.nicosb.uni.bloom_join.exception;

public class InvalidQueryException extends Exception {
    /**
	 * Indicates that the query is invalid.
	 */
	private static final long serialVersionUID = -3304981005174005400L;

	//Parameterless Constructor
    public InvalidQueryException() {}

    //Constructor that accepts a message
    public InvalidQueryException(String message)
    {
       super(message);
    }
}
