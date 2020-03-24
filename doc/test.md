# The Root Schema - Represents the mapping rom a DynamoDB table to an Exasol Virtual Schema table

_The root schema comprises the entire JSON document._

Type: `object`

<i id="#https://github.com/exasol/dynamodb-virtual-schema/blob/develop/src/main/resources/mappingLanguageSchema.json">path: #https://github.com/exasol/dynamodb-virtual-schema/blob/develop/src/main/resources/mappingLanguageSchema.json</i>

&#36;schema: [http://json-schema.org/draft-07/schema#](http://json-schema.org/draft-07/schema#)

<b id="httpsgithub.comexasoldynamodb-virtual-schemablobdevelopsrcmainresourcesmappinglanguageschema.json">&#36;id: https://github.com/exasol/dynamodb-virtual-schema/blob/develop/src/main/resources/mappingLanguageSchema.json</b>

This schema accepts additional properties.

**_Properties_**

 - <b id="#https://github.com/exasol/dynamodb-virtual-schema/blob/develop/src/main/resources/mappingLanguageSchema.json/properties/$schema">$schema</b> `required`
	 - Type: `string`
	 - <i id="#https://github.com/exasol/dynamodb-virtual-schema/blob/develop/src/main/resources/mappingLanguageSchema.json/properties/$schema">path: #https://github.com/exasol/dynamodb-virtual-schema/blob/develop/src/main/resources/mappingLanguageSchema.json/properties/$schema</i>
	 - The value is restricted to the following: 
		 1. _"https://github.com/exasol/dynamodb-virtual-schema/blob/develop/src/main/resources/mappingLanguageSchema.json"_
		 2. _"../../main/resources/mappingLanguageSchema.json"_
 - <b id="#https://github.com/exasol/dynamodb-virtual-schema/blob/develop/src/main/resources/mappingLanguageSchema.json/properties/srcTable">srcTable</b> `required`
	 - ### DynamoDB table name
	 - _the identifier of the DynamoDB table_
	 - Type: `string`
	 - <i id="#https://github.com/exasol/dynamodb-virtual-schema/blob/develop/src/main/resources/mappingLanguageSchema.json/properties/srcTable">path: #https://github.com/exasol/dynamodb-virtual-schema/blob/develop/src/main/resources/mappingLanguageSchema.json/properties/srcTable</i>
	 - Example values: 
		 1. _"MY_BOOKS"_
 - <b id="#https://github.com/exasol/dynamodb-virtual-schema/blob/develop/src/main/resources/mappingLanguageSchema.json/properties/destTableName">destTableName</b> `required`
	 - ### name of the table in Exasol Virtual Schema
	 - _the name of the resulting table in Exasol. The name must only contain [a-z A-Z 0-9]_
	 - Type: `string`
	 - <i id="#https://github.com/exasol/dynamodb-virtual-schema/blob/develop/src/main/resources/mappingLanguageSchema.json/properties/destTableName">path: #https://github.com/exasol/dynamodb-virtual-schema/blob/develop/src/main/resources/mappingLanguageSchema.json/properties/destTableName</i>
	 - Example values: 
		 1. _"BOOKS"_
 - <b id="#https://github.com/exasol/dynamodb-virtual-schema/blob/develop/src/main/resources/mappingLanguageSchema.json/properties/description">description</b>
	 - <i id="#https://github.com/exasol/dynamodb-virtual-schema/blob/develop/src/main/resources/mappingLanguageSchema.json/properties/description">path: #https://github.com/exasol/dynamodb-virtual-schema/blob/develop/src/main/resources/mappingLanguageSchema.json/properties/description</i>
	 - &#36;ref: [#/definitions/description](#/definitions/description)
 - <b id="#https://github.com/exasol/dynamodb-virtual-schema/blob/develop/src/main/resources/mappingLanguageSchema.json/properties/children">children</b> `required`
	 - <i id="#https://github.com/exasol/dynamodb-virtual-schema/blob/develop/src/main/resources/mappingLanguageSchema.json/properties/children">path: #https://github.com/exasol/dynamodb-virtual-schema/blob/develop/src/main/resources/mappingLanguageSchema.json/properties/children</i>
	 - &#36;ref: [#/definitions/children](#/definitions/children)
# definitions

 - ## The Description Schema
 - _An explanation about the purpose of this instance._
 - Type: `string`
 - <i id="#https://github.com/exasol/dynamodb-virtual-schema/blob/develop/src/main/resources/mappingLanguageSchema.json/definitions/description">path: #https://github.com/exasol/dynamodb-virtual-schema/blob/develop/src/main/resources/mappingLanguageSchema.json/definitions/description</i>
 - Type: `integer`
 - <i id="#https://github.com/exasol/dynamodb-virtual-schema/blob/develop/src/main/resources/mappingLanguageSchema.json/definitions/maxLength">path: #https://github.com/exasol/dynamodb-virtual-schema/blob/develop/src/main/resources/mappingLanguageSchema.json/definitions/maxLength</i>
 - Default: `254`
 - _An explanation about the purpose of this instance._
 - Type: `object`
 - <i id="#https://github.com/exasol/dynamodb-virtual-schema/blob/develop/src/main/resources/mappingLanguageSchema.json/definitions/children">path: #https://github.com/exasol/dynamodb-virtual-schema/blob/develop/src/main/resources/mappingLanguageSchema.json/definitions/children</i>
 - This schema <u>does not</u> accept additional properties.
 - Property Count:  &ge; 1
 - **_Properties_**
 - Type: `object`
 - <i id="#https://github.com/exasol/dynamodb-virtual-schema/blob/develop/src/main/resources/mappingLanguageSchema.json/definitions/child">path: #https://github.com/exasol/dynamodb-virtual-schema/blob/develop/src/main/resources/mappingLanguageSchema.json/definitions/child</i>
 - This schema accepts additional properties.
 - Property Count: between 1 and 1
 - **_Properties_**
	 - <b id="#https://github.com/exasol/dynamodb-virtual-schema/blob/develop/src/main/resources/mappingLanguageSchema.json/definitions/child/properties/children">children</b>
		 - #### object with no mapping
		 - <i id="#https://github.com/exasol/dynamodb-virtual-schema/blob/develop/src/main/resources/mappingLanguageSchema.json/definitions/child/properties/children">path: #https://github.com/exasol/dynamodb-virtual-schema/blob/develop/src/main/resources/mappingLanguageSchema.json/definitions/child/properties/children</i>
		 - &#36;ref: [#/definitions/children](#/definitions/children)
	 - <b id="#https://github.com/exasol/dynamodb-virtual-schema/blob/develop/src/main/resources/mappingLanguageSchema.json/definitions/child/properties/toStringMapping">toStringMapping</b>
		 - #### toString mapping definition
		 - Type: `object`
		 - <i id="#https://github.com/exasol/dynamodb-virtual-schema/blob/develop/src/main/resources/mappingLanguageSchema.json/definitions/child/properties/toStringMapping">path: #https://github.com/exasol/dynamodb-virtual-schema/blob/develop/src/main/resources/mappingLanguageSchema.json/definitions/child/properties/toStringMapping</i>
		 - This schema accepts additional properties.
		 - **_Properties_**
			 - <b id="#https://github.com/exasol/dynamodb-virtual-schema/blob/develop/src/main/resources/mappingLanguageSchema.json/definitions/child/properties/toStringMapping/properties/maxLength">maxLength</b>
				 - <i id="#https://github.com/exasol/dynamodb-virtual-schema/blob/develop/src/main/resources/mappingLanguageSchema.json/definitions/child/properties/toStringMapping/properties/maxLength">path: #https://github.com/exasol/dynamodb-virtual-schema/blob/develop/src/main/resources/mappingLanguageSchema.json/definitions/child/properties/toStringMapping/properties/maxLength</i>
				 - &#36;ref: [#/definitions/maxLength](#/definitions/maxLength)
			 - <b id="#https://github.com/exasol/dynamodb-virtual-schema/blob/develop/src/main/resources/mappingLanguageSchema.json/definitions/child/properties/toStringMapping/properties/overflow">overflow</b>
				 - <i id="#https://github.com/exasol/dynamodb-virtual-schema/blob/develop/src/main/resources/mappingLanguageSchema.json/definitions/child/properties/toStringMapping/properties/overflow">path: #https://github.com/exasol/dynamodb-virtual-schema/blob/develop/src/main/resources/mappingLanguageSchema.json/definitions/child/properties/toStringMapping/properties/overflow</i>
				 - The value is restricted to the following: 
					 1. _"TRUNCATE"_
					 2. _"ABORT"_
				 - Default: _"TRUNCATE"_
			 - <b id="#https://github.com/exasol/dynamodb-virtual-schema/blob/develop/src/main/resources/mappingLanguageSchema.json/definitions/child/properties/toStringMapping/properties/destName">destName</b>
				 - Type: `string`
				 - <i id="#https://github.com/exasol/dynamodb-virtual-schema/blob/develop/src/main/resources/mappingLanguageSchema.json/definitions/child/properties/toStringMapping/properties/destName">path: #https://github.com/exasol/dynamodb-virtual-schema/blob/develop/src/main/resources/mappingLanguageSchema.json/definitions/child/properties/toStringMapping/properties/destName</i>
			 - <b id="#https://github.com/exasol/dynamodb-virtual-schema/blob/develop/src/main/resources/mappingLanguageSchema.json/definitions/child/properties/toStringMapping/properties/description">description</b>
				 - <i id="#https://github.com/exasol/dynamodb-virtual-schema/blob/develop/src/main/resources/mappingLanguageSchema.json/definitions/child/properties/toStringMapping/properties/description">path: #https://github.com/exasol/dynamodb-virtual-schema/blob/develop/src/main/resources/mappingLanguageSchema.json/definitions/child/properties/toStringMapping/properties/description</i>
				 - &#36;ref: [#/definitions/description](#/definitions/description)
	 - <b id="#https://github.com/exasol/dynamodb-virtual-schema/blob/develop/src/main/resources/mappingLanguageSchema.json/definitions/child/properties/toJsonMapping">toJsonMapping</b>
		 - #### toJSON mapping definition
		 - <i id="#https://github.com/exasol/dynamodb-virtual-schema/blob/develop/src/main/resources/mappingLanguageSchema.json/definitions/child/properties/toJsonMapping">path: #https://github.com/exasol/dynamodb-virtual-schema/blob/develop/src/main/resources/mappingLanguageSchema.json/definitions/child/properties/toJsonMapping</i>
	 - <b id="#https://github.com/exasol/dynamodb-virtual-schema/blob/develop/src/main/resources/mappingLanguageSchema.json/definitions/child/properties/toTableMapping">toTableMapping</b>
		 - #### toTable mapping definition
		 - <i id="#https://github.com/exasol/dynamodb-virtual-schema/blob/develop/src/main/resources/mappingLanguageSchema.json/definitions/child/properties/toTableMapping">path: #https://github.com/exasol/dynamodb-virtual-schema/blob/develop/src/main/resources/mappingLanguageSchema.json/definitions/child/properties/toTableMapping</i>

_Generated with [json-schema-md-doc](https://brianwendt.github.io/json-schema-md-doc/)_