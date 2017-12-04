package org.wikidata.query.rdf.primarysources.curation;

import org.apache.http.client.fluent.Request;
import org.apache.http.client.utils.URIBuilder;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.openrdf.model.Literal;
import org.openrdf.model.Value;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.TupleQueryResultHandlerException;
import org.openrdf.query.resultio.QueryResultIO;
import org.openrdf.query.resultio.QueryResultParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.query.rdf.common.WikibaseDate;
import org.wikidata.query.rdf.common.WikibasePoint;
import org.wikidata.query.rdf.common.uri.GeoSparql;
import org.wikidata.query.rdf.common.uri.Provenance;
import org.wikidata.query.rdf.common.uri.WikibaseUris;
import org.wikidata.query.rdf.primarysources.WikibaseDataModelValidator;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.wikidata.query.rdf.primarysources.ingestion.UploadServlet.*;

/**
 * @author Marco Fossati - User:Hjfocs
 * @since 0.2.4
 * Created on Aug 04, 2017.
 */
public class SuggestServlet extends HttpServlet {

    static final String QID_PARAMETER = "qid";
    static final String DATASET_PARAMETER = "dataset";
    static final String IO_MIME_TYPE = "application/json";
    static final String DATASET_PLACE_HOLDER = "${DATASET}";
    static final String QID_PLACE_HOLDER = "${QID}";
    static final WikibaseUris WIKIBASE_URIS = WikibaseUris.getURISystem();
    static final String DEFAULT_GLOBE = "http://www.wikidata.org/entity/Q2";
    private static final String ONE_DATASET_QUERY =
            "SELECT ?property ?statement_node ?statement_property ?statement_value ?reference_property ?reference_value " +
                    "WHERE {" +
                    "  GRAPH <" + DATASET_PLACE_HOLDER + "> {" +
                    "    wd:" + QID_PLACE_HOLDER + " ?property ?statement_node ." +
                    "    ?statement_node ?statement_property ?statement_value ." +
                    "    OPTIONAL {" +
                    "      ?statement_value ?reference_property ?reference_value ." +
                    "    }" +
                    "  }" +
                    "}";
    private static final String ALL_DATASETS_QUERY =
            "SELECT ?dataset ?property ?statement_node ?statement_property ?statement_value ?reference_property ?reference_value " +
                    "WHERE {" +
                    "  GRAPH ?dataset {" +
                    "    wd:" + QID_PLACE_HOLDER + " ?property ?statement_node ." +
                    "    ?statement_node ?statement_property ?statement_value ." +
                    "    OPTIONAL {" +
                    "      ?statement_value ?reference_property ?reference_value ." +
                    "    }" +
                    "  }" +
                    "  FILTER STRENDS(str(?dataset), \"new\") ." +
                    "}";
    private static final Logger log = LoggerFactory.getLogger(SuggestServlet.class);
    private static final Object DEFAULT_ALTITUDE = null;
    private static final int DEFAULT_TIMEZONE = 0;
    private static final int DEFAULT_TIME_BEFORE = 0;
    private static final int DEFAULT_TIME_AFTER = 0;
    private static final int DEFAULT_TIME_PRECISION = 9;
    private static final String DEFAULT_CALENDAR_MODEL = "http://www.wikidata.org/entity/Q1985727";
    private static final String DEFAULT_UNIT = "1";
    private String dataset;
    private String qId;

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        boolean ok = processRequest(request, response);
        if (!ok) return;
        TupleQueryResult suggestions = getSuggestions();
        sendResponse(response, suggestions);
    }

    private boolean processRequest(HttpServletRequest request, HttpServletResponse response) throws IOException {
        WikibaseDataModelValidator validator = new WikibaseDataModelValidator();
        qId = request.getParameter(QID_PARAMETER);
        if (qId == null) {
            log.error("No QID given. Will fail with a bad request.");
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Missing required QID.");
            return false;
        } else if (!validator.isValidTerm(qId, "item")) {
            log.error("Invalid QID: {}. Will fail with a bad request.", qId);
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid QID: '" + qId + "'");
            return false;
        }
        String datasetParameter = request.getParameter(DATASET_PARAMETER);
        if (datasetParameter == null || datasetParameter.isEmpty()) {
            dataset = "all";
        } else {
            try {
                new URI(datasetParameter);
                dataset = datasetParameter;
            } catch (URISyntaxException use) {
                log.error("Invalid dataset URI: {}. Parse error at index {}. Will fail with a bad request", use.getInput(), use.getIndex());
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid dataset URI: <" + use.getInput() + ">. " +
                        "Parse error at index " + use.getIndex() + ".");
                return false;
            }
        }
        return true;
    }

    private TupleQueryResult getSuggestions() throws IOException {
        String query = dataset.equals("all") ? ALL_DATASETS_QUERY.replace(QID_PLACE_HOLDER, qId) : ONE_DATASET_QUERY.replace(QID_PLACE_HOLDER, qId).replace(DATASET_PLACE_HOLDER, dataset);
        return runSparqlQuery(query);
    }

    static TupleQueryResult runSparqlQuery(String query) throws IOException {
        URIBuilder builder = new URIBuilder();
        URI uri;
        try {
            uri = builder
                    .setScheme("http")
                    .setHost(BLAZEGRAPH_HOST)
                    .setPort(BLAZEGRAPH_PORT)
                    .setPath(BLAZEGRAPH_CONTEXT + BLAZEGRAPH_SPARQL_ENDPOINT)
                    .setParameter("query", query)
                    .build();
        } catch (URISyntaxException use) {
            log.error("Failed building the URI to query Blazegraph: {}. Parse error at index {}", use.getInput(), use.getIndex());
            return null;
        }
        InputStream results;
        results = Request.Get(uri)
                .setHeader("Accept", IO_MIME_TYPE)
                .execute()
                .returnContent().asStream();
        try {
            return QueryResultIO.parse(results, QueryResultIO.getParserFormatForMIMEType(IO_MIME_TYPE));
        } catch (QueryResultParseException qrpe) {
            log.error("Syntax error at line {}, column {} in the suggestion query: {}", query, qrpe.getLineNumber(), qrpe.getColumnNumber());
            return null;
        } catch (TupleQueryResultHandlerException tqrhe) {
            log.error("Something went wrong when handling the suggestion query: {}. Reason: {}", query, tqrhe.getMessage());
            return null;
        }
    }

    private void sendResponse(HttpServletResponse response, TupleQueryResult suggestions) throws IOException {
        response.setHeader("Access-Control-Allow-Origin", "*");
        JSONArray jsonSuggestions = formatSuggestions(suggestions);
        if (jsonSuggestions == null) {
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Something went wrong when retrieving suggestions.");
        } else if (jsonSuggestions.isEmpty()) {
            response.sendError(HttpServletResponse.SC_NOT_FOUND, "No suggestions available for item " + qId + " .");
        } else {
            response.setStatus(HttpServletResponse.SC_OK);
            response.setContentType(IO_MIME_TYPE);
            try (PrintWriter pw = response.getWriter()) {
                jsonSuggestions.writeJSONString(pw);
            }
            try (PrintWriter pw = response.getWriter()) {
                jsonSuggestions.writeJSONString(pw);
            }
        }
    }

    private JSONArray formatSuggestions(TupleQueryResult suggestions) {
        JSONArray jsonSuggestions = new JSONArray();
        String qualifierPrefix = WIKIBASE_URIS.property(WikibaseUris.PropertyType.QUALIFIER);
        String referencePrefix = Provenance.WAS_DERIVED_FROM;
        Map<String, StringBuilder> quickStatements = new HashMap<>();
        try {
            while (suggestions.hasNext()) {
                BindingSet suggestion = suggestions.next();
                Value datasetValue = suggestion.getValue("dataset");
                String currentDataset = datasetValue == null ? dataset : datasetValue.stringValue();
                String mainProperty = suggestion.getValue("property").stringValue().substring(WIKIBASE_URIS.property(WikibaseUris.PropertyType.CLAIM).length());
                String statementUuid = suggestion.getValue("statement_node").stringValue().substring(WIKIBASE_URIS.statement().length());
                String statementProperty = suggestion.getValue("statement_property").stringValue();
                Value statementValue = suggestion.getValue("statement_value");
                String qsKey = statementUuid + "|" + currentDataset;
                // Statement
                if (statementProperty.startsWith(WIKIBASE_URIS.property(WikibaseUris.PropertyType.STATEMENT))) {
                    StringBuilder quickStatement = quickStatements.getOrDefault(qsKey, new StringBuilder());
                    quickStatements.put(
                            qsKey,
                            quickStatement.insert(0, qId + "\t" + mainProperty + "\t" + rdfValueToQuickStatement(statementValue)));
                }
                // Qualifier
                else if (statementProperty.startsWith(qualifierPrefix)) {
                    StringBuilder quickStatement = quickStatements.getOrDefault(qsKey, new StringBuilder());
                    quickStatements.put(
                            qsKey,
                            quickStatement
                                    .append("\t")
                                    .append(statementProperty.substring(qualifierPrefix.length()))
                                    .append("\t")
                                    .append(rdfValueToQuickStatement(statementValue)));
                }
                // Reference
                else if (statementProperty.equals(referencePrefix)) {
                    String referenceProperty = suggestion.getValue("reference_property").stringValue();
                    Value referenceValue = suggestion.getValue("reference_value");
                    StringBuilder quickStatement = quickStatements.getOrDefault(qsKey, new StringBuilder());
                    quickStatements.put(
                            qsKey,
                            quickStatement
                                    .append("\t")
                                    .append(referenceProperty.substring(WIKIBASE_URIS.property(WikibaseUris.PropertyType.REFERENCE).length()).replace("P", "S"))
                                    .append("\t")
                                    .append(rdfValueToQuickStatement(referenceValue)));
                }
            }
        } catch (QueryEvaluationException qee) {
            log.error("Failed evaluating the suggestion query: {}", qee.getMessage());
            return null;
        }
        for (String key : quickStatements.keySet()) {
            String dataset = key.split("\\|")[1];
            String qs = quickStatements.get(key).toString();
            // The front end always thinks there is only 1 reference, so mint 1 QuickStatement per reference
            Pattern pattern = Pattern.compile("\tS\\d+\t[^\t]+");
            Matcher matcher = pattern.matcher(qs);
            List<String> references = new ArrayList<>();
            while (matcher.find()) {
                String reference = matcher.group();
                references.add(reference);
                qs = qs.replace(reference, "");
            }
            for (String ref : references) {
                String statement = qs + ref;
                JSONObject jsonSuggestion = new JSONObject();
                jsonSuggestion.put("dataset", dataset);
                jsonSuggestion.put("format", "QuickStatement");
                jsonSuggestion.put("state", "new");
                jsonSuggestion.put("statement", statement);
                jsonSuggestions.add(jsonSuggestion);
            }
        }
        return jsonSuggestions;
    }

    private JSONObject handleReference(BindingSet suggestion) {
        JSONObject jsonReference = new JSONObject();
        JSONObject forMediaWikiApi = new JSONObject();
        addCommonKeys(suggestion, jsonReference);
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

    private JSONObject handleStatement(BindingSet suggestion, String prefix, String property) {
        JSONObject jsonSuggestion = new JSONObject();
        JSONObject forMediaWikiApi = new JSONObject();
        addCommonKeys(suggestion, jsonSuggestion);
        String pId = property.substring(prefix.length());
        forMediaWikiApi.put("property", pId);
        forMediaWikiApi.put("snaktype", "value");
        Value value = suggestion.getValue("statement_value");
        String stringJsonValue = rdfValueToWikidataJson(value);
        forMediaWikiApi.put("value", stringJsonValue);
        jsonSuggestion.put("for_mw_api", forMediaWikiApi);
        return jsonSuggestion;
    }

    private void addCommonKeys(BindingSet suggestion, JSONObject jsonSuggestion) {
        if (!dataset.equals("all")) jsonSuggestion.put("dataset", dataset);
        else jsonSuggestion.put("dataset", suggestion.getValue("dataset").stringValue());
    }

    static String rdfValueToQuickStatement(Value value) {
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
            }
            // Time
            else if (dataType.equals(XMLSchema.DATETIME)) {
                WikibaseDate date = WikibaseDate.fromString(literal.getLabel());
                // The Blazegraph data loader converts '00' to '01'
                // Guess precision based on '01' values
                if (date.day() > 1) return date.toString() + "/11";
                else if (date.month() > 1) return date.toString() + "/10";
                else return date.toString() + "/9";
            }
            // Quantity
            else if (dataType.equals(XMLSchema.DECIMAL)) {
                return literal.getLabel();
            }
        }
        return null;
    }

    /**
     * Handle the data type and format the value to a JSON suitable for the Wikidata API.
     * See https://www.wikidata.org/wiki/Special:ListDatatypes
     *
     * @return the value as a String
     */
    private String rdfValueToWikidataJson(Value value) {
        JSONObject jsonValue = new JSONObject();
        if (value instanceof org.openrdf.model.URI) {
            org.openrdf.model.URI uri = (org.openrdf.model.URI) value;
            // Item
            if (uri.getNamespace().equals(WIKIBASE_URIS.entity())) {
                int id = Integer.parseInt(uri.getLocalName().replaceFirst("^Q", ""));
                jsonValue.put("entity-type", "item");
                jsonValue.put("numeric-id", id);
            }
            // URL
            else return value.stringValue();
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
            }
            // Globe coordinate
            else if (dataType.toString().equals(GeoSparql.WKT_LITERAL)) {
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
            }
            // Time
            else if (dataType.equals(XMLSchema.DATETIME)) {
                WikibaseDate date = WikibaseDate.fromString(literal.getLabel()).cleanWeirdStuff();
                jsonValue.put("time", date.toString());
                jsonValue.put("timezone", DEFAULT_TIMEZONE);
                jsonValue.put("before", DEFAULT_TIME_BEFORE);
                jsonValue.put("after", DEFAULT_TIME_AFTER);
                jsonValue.put("precision", DEFAULT_TIME_PRECISION);
                jsonValue.put("calendarmodel", DEFAULT_CALENDAR_MODEL);
            }
            // Quantity
            else if (dataType.equals(XMLSchema.DECIMAL)) {
                jsonValue.put("amount", literal.getLabel());
                jsonValue.put("unit", DEFAULT_UNIT);
            }
        }
        return jsonValue.toJSONString();
    }

    private double computeCoordinatesPrecision(String latitude, String longitude) {
        return Math.min(Math.pow(10, -numberOfDecimalDigits(latitude)), Math.pow(10, -numberOfDecimalDigits(longitude)));
    }

    private double numberOfDecimalDigits(String number) {
        String[] parts = number.split("\\.");
        return parts.length < 2 ? 0 : parts[1].length();
    }
}
