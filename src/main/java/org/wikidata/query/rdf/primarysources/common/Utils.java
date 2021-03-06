package org.wikidata.query.rdf.primarysources.common;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.Normalizer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.http.client.fluent.Request;
import org.apache.http.client.utils.URIBuilder;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.openrdf.model.Literal;
import org.openrdf.model.Model;
import org.openrdf.model.Resource;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.TupleQueryResultHandlerException;
import org.openrdf.query.resultio.QueryResultIO;
import org.openrdf.query.resultio.QueryResultParseException;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.Rio;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.query.rdf.common.WikibaseDate;
import org.wikidata.query.rdf.common.WikibasePoint;
import org.wikidata.query.rdf.common.uri.GeoSparql;
import org.wikidata.query.rdf.common.uri.Ontology;
import org.wikidata.query.rdf.common.uri.Provenance;
import org.wikidata.query.rdf.common.uri.WikibaseUris;

/**
 * A set of static utility methods used across services for pre- and post-processing purposes.
 *
 * @author Marco Fossati - <a href="https://meta.wikimedia.org/wiki/User:Hjfocs">User:Hjfocs</a>
 * @since 0.2.5 - created on Apr 17, 2018.
 */
public final class Utils {

    /**
     * Set of RDF namespaces to mint Wikidata URIs.
     */
    public static final WikibaseUris WIKIBASE_URIS = WikibaseUris.getURISystem();
    /**
     * Validator for the Wikidata RDF data model.
     */
    public static final WikibaseDataModelValidator VALIDATOR = new WikibaseDataModelValidator();

    private static final RDFFormat DEFAULT_RDF_FORMAT = RDFFormat.TURTLE;
    private static final String DEFAULT_GLOBE = "http://www.wikidata.org/entity/Q2";
    private static final Object DEFAULT_ALTITUDE = null;
    private static final int DEFAULT_TIMEZONE = 0;
    private static final int DEFAULT_TIME_BEFORE = 0;
    private static final int DEFAULT_TIME_AFTER = 0;
    private static final int DEFAULT_TIME_PRECISION = 9;
    private static final String DEFAULT_CALENDAR_MODEL = "http://www.wikidata.org/entity/Q1985727";
    private static final String DEFAULT_UNIT = "1";
    // Value data types matchers
    private static final Pattern TIME = Pattern.compile("^[+-]\\d+-\\d\\d-\\d\\dT\\d\\d:\\d\\d:\\d\\dZ/\\d+$");
    private static final Pattern LOCATION = Pattern.compile("^@([+\\-]?\\d+(?:.\\d+)?)/([+\\-]?\\d+(?:.\\d+))?$");
    private static final Pattern QUANTITY = Pattern.compile("^[+-]\\d+(\\.\\d+)?$");
    private static final Pattern MONOLINGUAL_TEXT = Pattern.compile("^(\\w+):(\"[^\"\\\\]*(?:\\\\.[^\"\\\\]*)*\")$");

    private static final Logger log = LoggerFactory.getLogger(Utils.class);

    private Utils() {
    }

    /**
     * Run a SPARQL query to the Blazegraph internal endpoint.
     *
     * @param query the SPARQL query.
     * @return the query result, or <i>null</i> if something goes wrong.
     */
    public static TupleQueryResult runSparqlQuery(String query) {
        log.debug("SPARQL query to be sent to Blazegraph: {}", query);
        URIBuilder builder = new URIBuilder();
        URI uri;
        try {
            uri = builder
                .setScheme("http")
                .setHost(Config.HOST)
                .setPort(Config.PORT)
                .setPath(Config.CONTEXT + Config.BLAZEGRAPH_SPARQL_ENDPOINT)
                .setParameter("query", query)
                .build();
        } catch (URISyntaxException use) {
            log.error("Failed building the URI to query Blazegraph: {}. Parse error at index {}", use.getInput(), use.getIndex());
            return null;
        }
        log.debug("URI built for Blazegraph SPARQL endpoint: {}", uri);
        InputStream results;
        try {
            results = Request.Get(uri)
                .setHeader("Accept", ApiParameters.DEFAULT_IO_CONTENT_TYPE)
                .execute()
                .returnContent().asStream();
        } catch (IOException ioe) {
            log.error("An I/O error occurred while sending the SPARQL query to Blazegraph. Query: " + query, ioe);
            return null;
        }
        try {
            TupleQueryResult result = QueryResultIO.parse(results, QueryResultIO.getParserFormatForMIMEType(ApiParameters.DEFAULT_IO_CONTENT_TYPE));
            log.debug("SPARQL query result: {}", result);
            return result;
        } catch (QueryResultParseException qrpe) {
            log.error("Syntax error at line {}, column {} in the SPARQL query: {}", query, qrpe.getLineNumber(), qrpe.getColumnNumber());
            return null;
        } catch (TupleQueryResultHandlerException tqrhe) {
            log.error("Something went wrong when handling the SPARQL query: " + query, tqrhe);
            return null;
        } catch (IOException ioe) {
            log.error("An I/O error occurred while reading the SPARQL results. Query: " + query, ioe);
            return null;
        }
    }

    /**
     * Convert a RDF value (i.e., the triple object) to a QuickStatement one.
     *
     * @param value the RDF value.
     * @return the QuickStatement value, or <i>null</i> if the RDF value does not map to any expected data type.
     */
    public static String rdfValueToQuickStatement(Value value) {
        if (value instanceof org.openrdf.model.URI) {
            org.openrdf.model.URI uri = (org.openrdf.model.URI) value;
            // Item
            if (uri.getNamespace().equals(WIKIBASE_URIS.entity())) return uri.getLocalName();
                // URL
            else return "\"" + value.stringValue() + "\"";
        } else if (value instanceof Literal) {
            Literal literal = (Literal) value;
            org.openrdf.model.URI dataType = literal.getDatatype();
            String language = literal.getLanguage();
            // String
            if (dataType == null && language == null) return "\"" + literal.stringValue() + "\"";
                // Monolingual text
            else if (language != null) return language + ":\"" + literal.getLabel() + "\"";
                // Globe coordinate
            else if (dataType.toString().equals(GeoSparql.WKT_LITERAL)) {
                WikibasePoint point = new WikibasePoint(literal.getLabel());
                String latitude = point.getLatitude();
                String longitude = point.getLongitude();
                return "@" + latitude + "/" + longitude;
            } else if (dataType.equals(XMLSchema.DATETIME)) { // Time
                WikibaseDate date = WikibaseDate.fromString(literal.getLabel());
                // The Blazegraph data loader converts '00' to '01'
                // Guess precision based on '01' values
                if (date.day() > 1) return date.toString() + "/11";
                else if (date.month() > 1) return date.toString() + "/10";
                else return date.toString() + "/9";
            } else if (dataType.equals(XMLSchema.DECIMAL)) { // Quantity
                return literal.getLabel();
            }
        }
        return null;
    }

    /**
     * Convert a SPARQL query result into a JSON with QuickStatement, suitable for the front end.
     *
     * @param queryResult the SPARQL query result.
     * @param datasetUri  the dataset URI.
     * @param subjectQid  the subject QID.
     * @return an array of JSON objects, each representing a statement suggestion to be curated; <i>null</i> if something went wrong.
     */
    public static JSONArray formatSuggestions(TupleQueryResult queryResult, String datasetUri, String subjectQid) {
        log.debug("Starting conversion of SPARQL results to QuickStatements");
        JSONArray jsonSuggestions = new JSONArray();
        String qualifierPrefix = WIKIBASE_URIS.property(WikibaseUris.PropertyType.QUALIFIER);
        String referencePrefix = Provenance.WAS_DERIVED_FROM;
        Map<String, StringBuilder> quickStatements = new HashMap<>();
        try {
            while (queryResult.hasNext()) {
                BindingSet suggestion = queryResult.next();
                Value datasetValue = suggestion.getValue("dataset");
                String currentDataset = datasetValue == null ? datasetUri : datasetValue.stringValue();
                String mainProperty = suggestion.getValue("property").stringValue().substring(WIKIBASE_URIS.property(WikibaseUris.PropertyType.CLAIM).length());
                String statementUuid = suggestion.getValue("statement_node").stringValue().substring(WIKIBASE_URIS.statement().length());
                String statementProperty = suggestion.getValue("statement_property").stringValue();
                Value statementValue = suggestion.getValue("statement_value");
                String qsKey = statementUuid + "|" + currentDataset;
                log.debug("Current QuickStatement key from RDF statement node and dataset: {}", qsKey);
                // Check statement, qualifier, reference
                if (statementProperty.startsWith(WIKIBASE_URIS.property(WikibaseUris.PropertyType.STATEMENT))) {
                    StringBuilder quickStatement = quickStatements.getOrDefault(qsKey, new StringBuilder());
                    String statement = subjectQid + "\t" + mainProperty + "\t" + rdfValueToQuickStatement(statementValue);
                    if (quickStatement.length() == 0)
                        log.debug("New key. Will start a new QuickStatement with statement: [{}]", statement);
                    else
                        log.debug("Existing key. Will update QuickStatement [{}] with statement [{}]", quickStatement, statement);
                    quickStatements.put(qsKey, quickStatement.insert(0, statement));
                } else if (statementProperty.startsWith(qualifierPrefix)) {
                    StringBuilder quickStatement = quickStatements.getOrDefault(qsKey, new StringBuilder());
                    String qualifier = "\t" + statementProperty.substring(qualifierPrefix.length()) + "\t" + rdfValueToQuickStatement(statementValue);
                    if (quickStatement.length() == 0)
                        log.debug("New key. Will start a new QuickStatement with qualifier: [{}]", qualifier);
                    else
                        log.debug("Existing key. Will update QuickStatement [{}] with qualifier [{}]", quickStatement, qualifier);
                    quickStatements.put(qsKey, quickStatement.append(qualifier));
                } else if (statementProperty.equals(referencePrefix)) {
                    String referenceProperty = suggestion.getValue("reference_property").stringValue();
                    Value referenceValue = suggestion.getValue("reference_value");
                    StringBuilder quickStatement = quickStatements.getOrDefault(qsKey, new StringBuilder());
                    String reference =
                        "\t" +
                            referenceProperty.substring(WIKIBASE_URIS.property(WikibaseUris.PropertyType.REFERENCE).length()).replace("P", "S") +
                            "\t" +
                            rdfValueToQuickStatement(referenceValue);
                    if (quickStatement.length() == 0)
                        log.debug("New key. Will start a new QuickStatement with reference: [{}]", reference);
                    else
                        log.debug("Existing key. Will update QuickStatement [{}] with reference [{}]", quickStatement, reference);
                    quickStatements.put(qsKey, quickStatement.append(reference));
                }
            }
        } catch (QueryEvaluationException qee) {
            log.error("Failed evaluating the suggestion query. The stack trace follows.", qee);
            return null;
        }
        log.debug("Converted QuickStatements: {}", quickStatements);
        for (String key : quickStatements.keySet()) {
            String dataset = key.split("\\|")[1];
            String qs = quickStatements.get(key).toString();
            JSONObject jsonSuggestion = new JSONObject();
            jsonSuggestion.put("dataset", dataset);
            jsonSuggestion.put("format", "QuickStatement");
            jsonSuggestion.put("state", "new");
            jsonSuggestion.put("statement", qs);
            jsonSuggestions.add(jsonSuggestion);
        }
        return jsonSuggestions;
    }

    /**
     * Convert a RDF value (i.e., the triple object) to a JSON suitable for the Wikidata API.
     * Handle the data type as per https://www.wikidata.org/wiki/Special:ListDatatypes
     *
     * @param value the RDF value.
     * @return the Wikidata API JSON <i>as a string</i>, since this is the weird way the API works.
     * Otherwise, an empty JSON string "<i>{}</i>" if the RDF value does not map to any expected data type.
     */
    public static String rdfValueToWikidataJson(Value value) {
        JSONObject jsonValue = new JSONObject();
        if (value instanceof org.openrdf.model.URI) {
            org.openrdf.model.URI uri = (org.openrdf.model.URI) value;
            // Item
            if (uri.getNamespace().equals(WIKIBASE_URIS.entity())) {
                int id = Integer.parseInt(uri.getLocalName().replaceFirst("^Q", ""));
                jsonValue.put("entity-type", "item");
                jsonValue.put("numeric-id", id);
            } else return value.stringValue(); // URL
        } else if (value instanceof Literal) {
            Literal literal = (Literal) value;
            org.openrdf.model.URI dataType = literal.getDatatype();
            String language = literal.getLanguage();
            // String
            if (dataType == null && language == null) return literal.stringValue();
                // Monolingual text
            else if (language != null) {
                jsonValue.put("language", language);
                jsonValue.put("text", literal.getLabel());
            } else if (dataType.toString().equals(GeoSparql.WKT_LITERAL)) { // Globe coordinate
                WikibasePoint point = new WikibasePoint(literal.getLabel());
                String latitude = point.getLatitude();
                String longitude = point.getLongitude();
                String globe = point.getGlobe();
                if (globe == null) globe = DEFAULT_GLOBE;
                jsonValue.put("latitude", Double.parseDouble(latitude));
                jsonValue.put("longitude", Double.parseDouble(longitude));
                jsonValue.put("precision", computeCoordinatesPrecision(latitude, longitude));
                jsonValue.put("globe", globe);
                jsonValue.put("altitude", DEFAULT_ALTITUDE);
            } else if (dataType.equals(XMLSchema.DATETIME)) { // Time
                WikibaseDate date = WikibaseDate.fromString(literal.getLabel()).cleanWeirdStuff();
                jsonValue.put("time", date.toString());
                jsonValue.put("timezone", DEFAULT_TIMEZONE);
                jsonValue.put("before", DEFAULT_TIME_BEFORE);
                jsonValue.put("after", DEFAULT_TIME_AFTER);
                jsonValue.put("precision", DEFAULT_TIME_PRECISION);
                jsonValue.put("calendarmodel", DEFAULT_CALENDAR_MODEL);
            } else if (dataType.equals(XMLSchema.DECIMAL)) { // Quantity
                jsonValue.put("amount", literal.getLabel());
                jsonValue.put("unit", DEFAULT_UNIT);
            }
        }
        return jsonValue.toJSONString();
    }

    /**
     * Run a SPARQL query against the Blazegraph internal endpoint to retrieve all new statements of a subject item that need curation.
     *
     * @param dataset    the dataset URI, or <i>all</i> to look into the whole database.
     * @param subjectQid the subject QID.
     * @return the query result, or <i>null</i> if something goes wrong.
     */
    public static TupleQueryResult getSuggestions(String dataset, String subjectQid) {
        String query = dataset.equals("all") ? SparqlQueries.SUGGEST_ALL_DATASETS_QUERY.replace(SparqlQueries.QID_PLACE_HOLDER, subjectQid) : SparqlQueries
            .SUGGEST_ONE_DATASET_QUERY.replace(SparqlQueries.QID_PLACE_HOLDER, subjectQid).replace(SparqlQueries.DATASET_PLACE_HOLDER, dataset);
        return runSparqlQuery(query);
    }

    /**
     * Convert a QuickStatement value to a RDF one.
     *
     * @param qsValue the QuickStatement value.
     * @return the RDF value.
     */
    public static Value quickStatementValueToRdf(String qsValue) {
        ValueFactory vf = ValueFactoryImpl.getInstance();
        if (VALIDATOR.isValidTerm(qsValue, "item")) {
            org.openrdf.model.URI item = vf.createURI(WIKIBASE_URIS.entity(), qsValue);
            log.debug("Item value. From QuickStatement [{}] to RDF [{}]", qsValue, item);
            return item;
        } else if (qsValue.matches(MONOLINGUAL_TEXT.pattern())) {
            Matcher matcher = MONOLINGUAL_TEXT.matcher(qsValue);
            matcher.matches();
            Literal monolingual = vf.createLiteral(matcher.group(2).replace("\"", ""), matcher.group(1));
            log.debug("Monolingual text value. From QuickStatement [{}] to RDF [{}]", qsValue, monolingual);
            return monolingual;
        } else if (qsValue.matches(TIME.pattern())) {
            String[] elements = qsValue.split("/");
            WikibaseDate wbTime = WikibaseDate.fromString(elements[0]);
            Literal time = vf.createLiteral(wbTime.toString(WikibaseDate.ToStringFormat.DATE_TIME), XMLSchema.DATETIME);
            log.debug("Time value. From QuickStatement [{}] to RDF [{}]", qsValue, time);
            return time;
        } else if (qsValue.matches(LOCATION.pattern())) {
            Matcher matcher = LOCATION.matcher(qsValue);
            matcher.matches();
            String[] latLong = new String[2];
            latLong[0] = matcher.group(1);
            latLong[1] = matcher.group(2);
            WikibasePoint point = new WikibasePoint(latLong, DEFAULT_GLOBE);
            Literal location = vf.createLiteral(point.toString(), GeoSparql.WKT_LITERAL);
            log.debug("Location value. From QuickStatement [{}] to RDF [{}]", qsValue, location);
            return location;
        } else if (qsValue.matches(QUANTITY.pattern())) {
            Literal quantity = vf.createLiteral(qsValue, XMLSchema.DECIMAL);
            log.debug("Quantity value. From QuickStatement [{}] to RDF [{}]", qsValue, quantity);
            return quantity;
        } else {
            String noQuotes = qsValue.replace("\"", "");
            try {
                // URL
                org.openrdf.model.URI url = vf.createURI(noQuotes);
                log.debug("URL value. From QuickStatement [{}] to RDF [{}]", qsValue, url);
                return url;
            } catch (IllegalArgumentException iae) {
                // Plain string
                Literal plain = vf.createLiteral(noQuotes);
                log.debug("Plain string value. From QuickStatement [{}] to RDF [{}]", qsValue, plain);
                return plain;
            }
        }
    }

    /**
     * Convert a Wikidata API JSON <i>reference</i> value to a RDF one.
     * Handle the data type as per the <a href="https://www.wikidata.org/wiki/Special:ListDatatypes">specifications</a>.
     *
     * @param jsonValue the Wikidata API JSON reference value.
     * @return the RDF value, or <i>null</i> if the Wikidata API JSON value does not map to any expected data type.
     */
    public static Value wikidataJsonReferenceValueToRdf(Object jsonValue) {
        ValueFactory vf = ValueFactoryImpl.getInstance();
        JSONParser p = new JSONParser();
        if (jsonValue instanceof JSONObject) {
            JSONObject actualValue = (JSONObject) jsonValue;
            JSONObject dataValue = (JSONObject) actualValue.get("datavalue");
            String dataValueType = (String) dataValue.get("type");
            JSONObject objectValue;
            String stringValue;
            switch (dataValueType) {
            case "wikibase-entityid":
                try {
                    objectValue = (JSONObject) p.parse((String) dataValue.get("value"));
                } catch (ParseException pe) {
                    log.error("Malformed reference JSON value. Parse error at index {}", pe.getPosition());
                    return null;
                }
                String id = Long.toString((long) objectValue.get("numeric-id"));
                return vf.createURI(WIKIBASE_URIS.entity(), "Q" + id);
            case "string":
                stringValue = (String) dataValue.get("value");
                try {
                    // URL
                    return vf.createURI(stringValue);
                } catch (IllegalArgumentException iae) {
                    // String
                    return vf.createLiteral(stringValue);
                }
            case "monolingualtext":
                try {
                    objectValue = (JSONObject) p.parse((String) dataValue.get("value"));
                } catch (ParseException pe) {
                    log.error("Malformed reference JSON value. Parse error at index {}", pe.getPosition());
                    return null;
                }
                return vf.createLiteral((String) objectValue.get("text"), (String) objectValue.get("language"));
            }
        } else return null;
        return null;
    }

    /**
     * Convert a Wikidata API JSON value to a RDF one.
     * Handle the data type as per the <a href="https://www.wikidata.org/wiki/Special:ListDatatypes">specifications</a>.
     *
     * @param jsonValue the Wikidata API JSON value.
     * @return the RDF value, or <i>null</i> if the Wikidata API JSON value does not map to any expected data type.
     */
    public static Value wikidataJsonValueToRdf(Object jsonValue) {
        ValueFactory vf = ValueFactoryImpl.getInstance();
        JSONParser parser = new JSONParser();
        // Values that are JSON objects are passed to the MW API as strings, cool!
        JSONObject jsonObjectValue;
        if (jsonValue instanceof String) {
            String stringValue = (String) jsonValue;
            try {
                jsonObjectValue = (JSONObject) parser.parse(stringValue);
            } catch (ParseException pe) {
                // Yes, values explicitly typed as strings may also be URLs, nice!
                try {
                    // URL
                    return vf.createURI(stringValue);
                } catch (IllegalArgumentException iae) {
                    // String
                    return vf.createLiteral(stringValue);
                }
            }
        } else return null;
        // Item
        if (jsonObjectValue.containsValue("item")) {
            String id = Long.toString((long) jsonObjectValue.get("numeric-id"));
            return vf.createURI(WIKIBASE_URIS.entity(), "Q" + id);
        } else if (jsonObjectValue.containsKey("language")) { // Monolingual text
            return vf.createLiteral((String) jsonObjectValue.get("text"), (String) jsonObjectValue.get("language"));
        } else if (jsonObjectValue.containsKey("globe")) { // Globe coordinate
            String[] latLong = new String[2];
            latLong[0] = Double.toString((double) jsonObjectValue.get("latitude"));
            latLong[1] = Double.toString((double) jsonObjectValue.get("longitude"));
            WikibasePoint point = new WikibasePoint(latLong, (String) jsonObjectValue.get("globe"));
            return vf.createLiteral(point.toString(), GeoSparql.WKT_LITERAL);
        } else if (jsonObjectValue.containsKey("time")) { // Time
            WikibaseDate date = WikibaseDate.fromString((String) jsonObjectValue.get("time"));
            return vf.createLiteral(date.toString(WikibaseDate.ToStringFormat.DATE_TIME), XMLSchema.DATETIME);
        } else if (jsonObjectValue.containsKey("amount")) { // Quantity
            return vf.createLiteral((String) jsonObjectValue.get("amount"), XMLSchema.DECIMAL);
        }
        return null;
    }

    /**
     * Validate a Wiki user name.
     * URI-reserved characters are not allowed, see the <a href="https://tools.ietf.org/html/rfc3986#section-2.2">specifications</a>.
     *
     * @param userName the Wiki user name.
     * @return <i>true</i> if valid, <i>false</i> otherwise.
     */
    public static boolean validateUserName(String userName) {
        Pattern illegal = Pattern.compile("[:/?#\\[\\]@!$&'()*+,;=]");
        Matcher matcher = illegal.matcher(userName);
        if (matcher.find()) {
            log.warn("Illegal characters found in the user name: {}", userName);
            return false;
        }
        return true;
    }

    /**
     * Add an <i>instance-of</i> quad to each subject node in a RDF dataset. For instance:
     * <br>
     * {@code wd:Q666 rdf:type wikibase:Item <http://strephit/new>}
     * <br>
     * This should optimize the execution of queries on subject items.
     *
     * @param dataset    the RDF dataset.
     * @param datasetUri the dataset URI.
     */
    public static void addTypeToSubjectItems(Model dataset, String datasetUri) {
        Set<Resource> subjects = dataset.subjects();
        Set<org.openrdf.model.URI> items = new HashSet<>();
        for (Resource s : subjects) {
            org.openrdf.model.URI subject = (org.openrdf.model.URI) s;
            if (subject.getNamespace().equals(WIKIBASE_URIS.entity())) items.add(subject);
        }
        for (org.openrdf.model.URI item : items)
            dataset.add(item, RDF.TYPE, new URIImpl(Ontology.ITEM), new URIImpl(datasetUri));
        log.debug("Added a (item, rdf:type, wikibase:Item) triple to each subject item: {}", items);
    }

    /**
     * Try to find a suitable RDF format for a given file name.
     * If the part has no content type, guess the format based on the file name extension.
     * Fall back to <i>Turtle</i> if the guess fails, as we cannot blame the client for uploading proper content with an arbitrary (or no) extension.
     *
     * @param contentType the eventual content type declared in a HTTP request header.
     * @param fileName    the dataset file name.
     * @return the RDF format.
     */
    public static RDFFormat handleRdfFormat(String contentType, String fileName) {
        // If the content type is not explicilty RDF, still try to guess based on the file name extension
        if (contentType == null) return Rio.getParserFormatForFileName(fileName, DEFAULT_RDF_FORMAT);
        else return Rio.getParserFormatForMIMEType(contentType, Rio.getParserFormatForFileName(fileName));
    }

    /**
     * Build a sanitized ASCII URI out of a given dataset name.
     *
     * @param datasetName the dataset name.
     * @return the sanitized dataset URI.
     */
    public static String mintDatasetURI(String datasetName) {
        // Delete any character that is not a letter, a number, or a whitespace, as we want to mint readable URIs
        String onlyLetters = datasetName.replaceAll("[^\\p{L}\\d\\s]", "");
        // Remove diactrics to mint ASCII URIs
        String noDiacritics = Normalizer.normalize(onlyLetters, Normalizer.Form.NFD).replaceAll("[^\\p{ASCII}]", "");
        // Replace whitespaces with a dash
        String clean = noDiacritics.replaceAll("\\s+", "-");
        // A freshly uploaded dataset gets the "/new" state
        String datasetURI = "http://" + clean.toLowerCase(Locale.ENGLISH) + "/new";
        log.info("Named graph URI: {}", datasetURI);
        return datasetURI;
    }

    private static double computeCoordinatesPrecision(String latitude, String longitude) {
        return Math.min(Math.pow(10, -numberOfDecimalDigits(latitude)), Math.pow(10, -numberOfDecimalDigits(longitude)));
    }

    private static double numberOfDecimalDigits(String number) {
        String[] parts = number.split("\\.");
        return parts.length < 2 ? 0 : parts[1].length();
    }

    private static void addDatasetKey(BindingSet suggestion, JSONObject jsonSuggestion, String dataset) {
        if (!dataset.equals("all")) jsonSuggestion.put("dataset", dataset);
        else jsonSuggestion.put("dataset", suggestion.getValue("dataset").stringValue());
    }

    private static JSONObject rdfReferenceToWikidataJson(BindingSet suggestion, String dataset) {
        JSONObject jsonReference = new JSONObject();
        JSONObject forMediaWikiApi = new JSONObject();
        addDatasetKey(suggestion, jsonReference, dataset);
        JSONObject snaks = new JSONObject();
        JSONArray values = new JSONArray();
        JSONObject dataValue = new JSONObject();
        JSONObject finalValue = new JSONObject();
        String referencePid = suggestion.getValue("reference_property").stringValue()
            .substring(WIKIBASE_URIS.property(WikibaseUris.PropertyType.REFERENCE).length());
        Value referenceValue = suggestion.getValue("reference_value");
        String finalValueType = null;
        if (referenceValue instanceof org.openrdf.model.URI) {
            org.openrdf.model.URI uri = (org.openrdf.model.URI) referenceValue;
            // Yes, it's true, URLs have type "string"
            finalValueType = uri.getNamespace().equals(WIKIBASE_URIS.entity()) ? "wikibase-entityid" : "string";
        } else if (referenceValue instanceof Literal) {
            Literal literal = (Literal) referenceValue;
            finalValueType = literal.getLanguage() == null ? "string" : "monolingualtext";
        }
        finalValue.put("type", finalValueType);
        finalValue.put("value", rdfValueToWikidataJson(referenceValue));
        dataValue.put("snaktype", "value");
        dataValue.put("property", referencePid);
        dataValue.put("datavalue", finalValue);
        values.add(dataValue);
        snaks.put(referencePid, values);
        forMediaWikiApi.put("snaks", snaks);
        jsonReference.put("for_mw_api", forMediaWikiApi);
        return jsonReference;
    }

    private static JSONObject RdfStatementToWikidataJson(BindingSet suggestion, String prefix, String property, String dataset) {
        JSONObject jsonSuggestion = new JSONObject();
        JSONObject forMediaWikiApi = new JSONObject();
        addDatasetKey(suggestion, jsonSuggestion, dataset);
        String pId = property.substring(prefix.length());
        forMediaWikiApi.put("property", pId);
        forMediaWikiApi.put("snaktype", "value");
        Value value = suggestion.getValue("statement_value");
        String stringJsonValue = rdfValueToWikidataJson(value);
        forMediaWikiApi.put("value", stringJsonValue);
        jsonSuggestion.put("for_mw_api", forMediaWikiApi);
        return jsonSuggestion;
    }
}
