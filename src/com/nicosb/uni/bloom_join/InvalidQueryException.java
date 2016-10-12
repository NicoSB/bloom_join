package com.nicosb.uni.bloom_join;

public class InvalidQueryException extends Exception {
    //Parameterless Constructor
    public InvalidQueryException() {}

    //Constructor that accepts a message
    public InvalidQueryException(String message)
    {
       super(message);
    }
}
