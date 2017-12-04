package org.wikidata.query.rdf.primarysources.curation;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.query.rdf.common.uri.Provenance;
import org.wikidata.query.rdf.common.uri.WikibaseUris;
import org.wikidata.query.rdf.primarysources.WikibaseDataModelValidator;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.wikidata.query.rdf.primarysources.curation.SuggestServlet.*;

/**
 * @author Marco Fossati - User:Hjfocs
 * @since 0.2.4
 * Created on Nov 20, 2017.
 */
public class SearchServlet extends HttpServlet {
    private static final String PROPERTY_PLACE_HOLDER = "${PROPERTY}";
    private static final String OFFSET_PLACE_HOLDER = "${OFFSET}";
    private static final String LIMIT_PLACE_HOLDER = "${LIMIT}";
    private static final String ONE_DATASET_QUERY =
            "SELECT * " +
                    "WHERE {" +
                    "  GRAPH <" + DATASET_PLACE_HOLDER + "> {" +
                    "    ?item " + PROPERTY_PLACE_HOLDER + " ?statement_node ." +
                    "    ?statement_node ?statement_property ?statement_value ." +
                    "    OPTIONAL {" +
                    "      ?statement_value ?reference_property ?reference_value ." +
                    "    }" +
                    "  }" +
                    "  FILTER STRSTARTS(str(?item), \"http://www.wikidata.org/entity/Q\") ." +
                    "}" +
                    "OFFSET " + OFFSET_PLACE_HOLDER + " " +
                    "LIMIT " + LIMIT_PLACE_HOLDER;
    private static final String ALL_DATASETS_QUERY =
            "SELECT * " +
                    "WHERE {" +
                    "  GRAPH ?dataset {" +
                    "    ?item " + PROPERTY_PLACE_HOLDER + " ?statement_node ." +
                    "    ?statement_node ?statement_property ?statement_value ." +
                    "    OPTIONAL {" +
                    "      ?statement_value ?reference_property ?reference_value ." +
                    "    }" +
                    "  }" +
                    "  FILTER STRSTARTS(str(?item), \"http://www.wikidata.org/entity/Q\") ." +
                    "  FILTER STRENDS(str(?dataset), \"new\") ." +
                    "}" +
                    "OFFSET " + OFFSET_PLACE_HOLDER + " " +
                    "LIMIT " + LIMIT_PLACE_HOLDER;
    private static final String OFFSET_PARAMETER = "offset";
    private static final String LIMIT_PARAMETER = "limit";
    private static final String PROPERTY_PARAMETER = "property";
    private String dataset;
    private String property;
    // N.B.: SPARQL offset and limit values are function of these, see #getSuggestions
    private int offset = 0;
    private int limit = 50;

    private static final Logger log = LoggerFactory.getLogger(SearchServlet.class);

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        boolean ok = processRequest(request, response);
        if (!ok) return;
        TupleQueryResult suggestions = getSuggestions();
        sendResponse(response, suggestions);
    }

    private boolean processRequest(HttpServletRequest request, HttpServletResponse response) throws IOException {
        WikibaseDataModelValidator validator = new WikibaseDataModelValidator();
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
        String propertyParameter = request.getParameter(PROPERTY_PARAMETER);
        if (propertyParameter == null || propertyParameter.isEmpty()) {
            property = "all";
        } else {
            if (!validator.isValidTerm(propertyParameter, "property")) {
                log.error("Invalid PID: {}. Will fail with a bad request.", propertyParameter);
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid PID: '" + propertyParameter + "'");
                return false;
            }
        }
        String offsetParameter = request.getParameter(OFFSET_PARAMETER);
        if (offsetParameter != null && !offsetParameter.isEmpty()) {
            try {
                offset = Integer.parseInt(offsetParameter);
            } catch (NumberFormatException nfe) {
                log.error("Invalid offset: {}. Does not look like an integer number. Will fail with a bad request", offsetParameter);
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid offset: " + offsetParameter + ". " +
                        "Does not look like an integer number.");
                return false;
            }
        }
        String limitParameter = request.getParameter(LIMIT_PARAMETER);
        if (limitParameter != null && !limitParameter.isEmpty()) {
            try {
                limit = Integer.parseInt(limitParameter);
            } catch (NumberFormatException nfe) {
                log.error("Invalid limit: {}. Does not look like an integer number. Will fail with a bad request", limitParameter);
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid limit: " + limitParameter + ". " +
                        "Does not look like an integer number.");
                return false;
            }
        }
        return true;
    }

    private void sendResponse(HttpServletResponse response, TupleQueryResult suggestions) throws IOException {
        response.setHeader("Access-Control-Allow-Origin", "*");
        JSONArray jsonSuggestions = formatSuggestions(suggestions);
        if (jsonSuggestions == null) {
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Something went wrong when retrieving suggestions.");
        } else if (jsonSuggestions.isEmpty()) {
            response.sendError(HttpServletResponse.SC_NOT_FOUND, "No suggestions available .");
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

    private TupleQueryResult getSuggestions() throws IOException {
        String query;
        query = dataset.equals("all") ? ALL_DATASETS_QUERY : ONE_DATASET_QUERY.replace(DATASET_PLACE_HOLDER, dataset);
        query = query.replace(OFFSET_PLACE_HOLDER, String.valueOf(offset)).replace(LIMIT_PLACE_HOLDER, String.valueOf(limit));
        query = property.equals("all") ? query.replace(PROPERTY_PLACE_HOLDER, "?property") : query.replace(PROPERTY_PLACE_HOLDER, "p:" + property);
        return runSparqlQuery(query);
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
                String subject = suggestion.getValue("item").stringValue().substring(WIKIBASE_URIS.entity().length());
                Value mainPropertyValue = suggestion.getValue("property");
                String mainProperty = mainPropertyValue == null ? property : mainPropertyValue.stringValue().substring(WIKIBASE_URIS.property(WikibaseUris.PropertyType.CLAIM).length());
                String statementUuid = suggestion.getValue("statement_node").stringValue().substring(WIKIBASE_URIS.statement().length());
                String statementProperty = suggestion.getValue("statement_property").stringValue();
                Value statementValue = suggestion.getValue("statement_value");
                String qsKey = statementUuid + "|" + currentDataset;
                // Statement
                if (statementProperty.startsWith(WIKIBASE_URIS.property(WikibaseUris.PropertyType.STATEMENT))) {
                    StringBuilder quickStatement = quickStatements.getOrDefault(qsKey, new StringBuilder());
                    quickStatements.put(
                            qsKey,
                            quickStatement.insert(0, subject + "\t" + mainProperty + "\t" + rdfValueToQuickStatement(statementValue)));
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
            // The front end always thinks there is only 1 reference, so mint 1 output QuickStatement per reference
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
}
