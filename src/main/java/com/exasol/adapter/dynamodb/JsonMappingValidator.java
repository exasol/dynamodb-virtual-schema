package com.exasol.adapter.dynamodb;

import com.exasol.adapter.AdapterException;
import org.everit.json.schema.Schema;
import org.everit.json.schema.ValidationException;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;
import org.json.JSONTokener;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public class JsonMappingValidator {
    public void validate(final JSONObject schemaMappingDefinition) throws IOException, MappingException {
        final ClassLoader classLoader = JsonMappingProvider.class.getClassLoader();
        try (final InputStream inputStream = classLoader.getResourceAsStream("mappingLanguageSchema.json")) {
            final JSONObject rawSchema = new JSONObject(new JSONTokener(inputStream));
            final Schema schema = SchemaLoader.load(rawSchema);
            schema.validate(schemaMappingDefinition);
        }catch (final ValidationException e){
            throw new MappingException(extractReadableErrorMessage(e));
        }
    }

    private String extractReadableErrorMessage(final ValidationException e){
        final List<ValidationException> causingExceptions = e.getCausingExceptions();
        if(!causingExceptions.isEmpty()){
            final ValidationException firstException = causingExceptions.get(0);
            if(firstException.getKeyword().equals("anyOf")){
                return  firstException.getPointerToViolation() + " unknown mapping type";
            }
            return firstException.getMessage();
        }
        return e.getErrorMessage();
    }

    public static class MappingException extends Exception{
        private MappingException(){}
        public MappingException(final String message){
            super(message);
        }
    }

}
