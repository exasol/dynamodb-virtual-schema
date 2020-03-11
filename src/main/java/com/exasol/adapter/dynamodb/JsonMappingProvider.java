package com.exasol.adapter.dynamodb;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.dynamodb.mapping_definition.TableMappingDefinition;
import com.exasol.adapter.dynamodb.mapping_definition.result_walker.DynamodbResultWalker;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;

import javax.print.DocFlavor;
import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * This {@link MappingProvider} reads a {@link SchemaMappingDefinition} from json files.
 * The json files must follow the schema defined at /schemaMapping/schema.
 * Documentation on schema mapping definitions can be found at /doc/schemaMappingLanguageReference.md
 */
public class JsonMappingProvider implements MappingProvider {
    private static final List<String> SUPPORTED_SCHEMAS = List.of("https://github.com/exasol/dynamodb-virtual-schema/tree/master/schemaMapping/schema");
    private static final String SCHEMA_KEY = "$schema";
    private static final String DEST_TABLE_NAME_KEY = "destTableName";
    private static final String SRC_TABLE_KEY = "srcTable";
    private static final String DESCRIPTION_KEY = "description";
    private static final String CHILDREN_KEY = "children";

    private final List<TableMappingDefinition> tables = new ArrayList<>();

    public JsonMappingProvider(final File definitionsPath) throws IOException {
        this(splitIfDirectory(definitionsPath));
    }

    /**
     * If the given definitionsPath is an directory all json files are returned.
     * @param definitionsPath
     * @return
     */
    private static File[] splitIfDirectory(final File definitionsPath){
        final String jsonFileEnding = ".json";
        if(definitionsPath.isFile()){
            return new File[]{definitionsPath};
        }
        else{
            return definitionsPath.listFiles((file, fileName) -> fileName.endsWith(jsonFileEnding));
        }
    }

    private JsonMappingProvider(final File[] definitionsPaths) throws IOException {
        for(final File definitionPath : definitionsPaths){
            try (final InputStream inputStream = new FileInputStream(definitionPath)) {
               addRootDefinition(new JSONObject(new JSONTokener(inputStream)), definitionPath.getName());
            }
        }
    }

    private void addRootDefinition(final JSONObject definition, final String fileName) {
        final TableMappingDefinition.Builder definitionBuilder = TableMappingDefinition.builder(definition.getString(DEST_TABLE_NAME_KEY),true);
        //TODO add columns
        this.tables.add(definitionBuilder.build());
    }


    private void walk(final DynamodbResultWalker walkerToThisPath, final JSONObject definition){

    }

    private void walk(final DynamodbResultWalker walkerToThisPath, final JSONArray definition){

    }


    @Override
    public SchemaMappingDefinition getSchemaMapping() {
        return new SchemaMappingDefinition(this.tables);
    }


}
