package com.floragunn.codova.documents;

import com.fasterxml.jackson.core.JsonProcessingException;

public class UnexpectedDocumentStructureException extends JsonProcessingException {

    private static final long serialVersionUID = 4969591600760212956L;

    protected UnexpectedDocumentStructureException(String msg) {
        super(msg);
    }

}
