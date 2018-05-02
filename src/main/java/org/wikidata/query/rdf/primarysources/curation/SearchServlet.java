package org.wikidata.query.rdf.primarysources.curation;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

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
import org.wikidata.query.rdf.primarysources.common.ApiParameters;
import org.wikidata.query.rdf.primarysources.common.SparqlQueries;
import org.wikidata.query.rdf.primarysources.common.Utils;
import org.wikidata.query.rdf.primarysources.common.WikibaseDataModelValidator;

/**
 * Search statements to be curated, with optional filters on properties and item values.
 * <p>
 * The output statements are serialized in <i>QuickStatements</i>.
 * See the <a href="https://www.wikidata.org/wiki/Help:QuickStatements#Command_sequence_syntax">syntax specifications</a>.
 * <p>
 * Support for <i>Wikidata JSON</i> output can be implemented as a method similar to the private one
 * {@code formatSearchSuggestions} in {@code sendResponse}.
 * The conversion logic between Wikidata RDF and Wikidata JSON is already available, see for instance {@link Utils#rdfValueToWikidataJson(Value)}.
 * <p>
 * This service is part of the Wikidata primary sources tool <i>Curation API</i>:
 * see <a href="https://upload.wikimedia.org/wikipedia/commons/a/a7/Wikidata_primary_sources_tool_architecture_v2.svg">this picture</a>
 * for an overview of the tool architecture.
 *
 * @author Marco Fossati - <a href="https://meta.wikimedia.org/wiki/User:Hjfocs">User:Hjfocs</a>
 * @since 0.2.5 - created on Nov 20, 2017.
 */
public class SearchServlet extends HttpServlet {

    private static final int DEFAULT_OFFSET = 0;
    private static final int DEFAULT_LIMIT = 50;
    private static final Logger log = LoggerFactory.getLogger(SearchServlet.class);

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        RequestParameters parameters = new RequestParameters();
        boolean ok = processRequest(request, parameters, response);
        if (!ok) return;
        log.debug("Required parameters stored as fields in private class: {}", parameters);
        TupleQueryResult suggestions = getSearchSuggestions(parameters);
        sendResponse(response, suggestions, parameters);
        log.info("GET /search successful");
    }

    private boolean processRequest(HttpServletRequest request, RequestParameters parameters, HttpServletResponse response) throws IOException {
        WikibaseDataModelValidator validator = new WikibaseDataModelValidator();
        String datasetParameter = request.getParameter(ApiParameters.DATASET_PARAMETER);
        if (datasetParameter == null || datasetParameter.isEmpty()) {
            parameters.dataset = "all";
        } else {
            try {
                new URI(datasetParameter);
                parameters.dataset = datasetParameter;
            } catch (URISyntaxException use) {
                log.warn("Invalid dataset URI: {}. Parse error at index {}. Will fail with a bad request", use.getInput(), use.getIndex());
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid dataset URI: <" + use.getInput() + ">. " +
                    "Parse error at index " + use.getIndex() + ".");
                return false;
            }
        }
        String propertyParameter = request.getParameter(ApiParameters.PROPERTY_PARAMETER);
        if (propertyParameter == null || propertyParameter.isEmpty()) {
            parameters.property = "all";
        } else {
            if (!validator.isValidTerm(propertyParameter, "property")) {
                log.warn("Invalid PID: {}. Will fail with a bad request.", propertyParameter);
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid PID: '" + propertyParameter + "'");
                return false;
            }
            parameters.property = propertyParameter;
        }
        String valueParameter = request.getParameter(ApiParameters.VALUE_PARAMETER);
        if (valueParameter == null || valueParameter.isEmpty()) {
            parameters.value = null;
        } else {
            if (!validator.isValidTerm(valueParameter, "item")) {
                log.warn("Invalid QID: {}. The value must be a Wikidata item. Will fail with a bad request.", valueParameter);
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid QID: '" + valueParameter + "'" +
                    "The value must be a Wikidata item.");
                return false;
            }
            parameters.value = valueParameter;
        }
        String offsetParameter = request.getParameter(ApiParameters.OFFSET_PARAMETER);
        if (offsetParameter == null || offsetParameter.isEmpty()) parameters.offset = DEFAULT_OFFSET;
        else {
            try {
                parameters.offset = Integer.parseInt(offsetParameter);
            } catch (NumberFormatException nfe) {
                log.warn("Invalid offset: {}. Does not look like an integer number. Will fail with a bad request", offsetParameter);
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid offset: " + offsetParameter + ". " +
                    "Does not look like an integer number.");
                return false;
            }
        }
        String limitParameter = request.getParameter(ApiParameters.LIMIT_PARAMETER);
        if (limitParameter == null || limitParameter.isEmpty()) parameters.limit = DEFAULT_LIMIT;
        else {
            try {
                parameters.limit = Integer.parseInt(limitParameter);
            } catch (NumberFormatException nfe) {
                log.warn("Invalid limit: {}. Does not look like an integer number. Will fail with a bad request", limitParameter);
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid limit: " + limitParameter + ". " +
                    "Does not look like an integer number.");
                return false;
            }
        }
        return true;
    }

    private void sendResponse(HttpServletResponse response, TupleQueryResult suggestions, RequestParameters parameters) throws IOException {
        JSONArray jsonSuggestions = formatSearchSuggestions(suggestions, parameters);
        if (jsonSuggestions == null) {
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Something went wrong when retrieving suggestions.");
        } else if (jsonSuggestions.isEmpty()) {
            log.warn("No search suggestions available. Will fail with a not found");
            response.sendError(HttpServletResponse.SC_NOT_FOUND, "No suggestions available .");
        } else {
            response.setStatus(HttpServletResponse.SC_OK);
            response.setContentType(ApiParameters.DEFAULT_IO_CONTENT_TYPE);
            try (PrintWriter pw = response.getWriter()) {
                jsonSuggestions.writeJSONString(pw);
            }
        }
    }

    private TupleQueryResult getSearchSuggestions(RequestParameters parameters) {
        String query;
        if (parameters.dataset.equals("all")) {
            if (parameters.value == null) query = SparqlQueries.SEARCH_ALL_DATASETS_QUERY;
            else
                query = SparqlQueries.SEARCH_ALL_DATASETS_VALUE_QUERY.replace(SparqlQueries.ITEM_VALUE_PLACE_HOLDER, parameters.value);
        } else {
            if (parameters.value == null)
                query = SparqlQueries.SEARCH_ONE_DATASET_QUERY.replace(SparqlQueries.DATASET_PLACE_HOLDER, parameters.dataset);
            else
                query = SparqlQueries.SEARCH_ONE_DATASET_VALUE_QUERY.replace(SparqlQueries.DATASET_PLACE_HOLDER, parameters.dataset).replace(SparqlQueries
                    .ITEM_VALUE_PLACE_HOLDER, parameters.value);
        }
        query = query.replace(SparqlQueries.OFFSET_PLACE_HOLDER, String.valueOf(parameters.offset)).replace(SparqlQueries.LIMIT_PLACE_HOLDER, String.valueOf(
            parameters.limit));
        query = parameters.property.equals("all") ? query.replace(SparqlQueries.PROPERTY_PLACE_HOLDER, "?property") : query.replace(SparqlQueries
            .PROPERTY_PLACE_HOLDER, "p:" + parameters.property);
        return Utils.runSparqlQuery(query);
    }

    private JSONArray formatSearchSuggestions(TupleQueryResult suggestions, RequestParameters parameters) {
        log.debug("Starting conversion of SPARQL results to QuickStatements");
        JSONArray jsonSuggestions = new JSONArray();
        String qualifierPrefix = Utils.WIKIBASE_URIS.property(WikibaseUris.PropertyType.QUALIFIER);
        String referencePrefix = Provenance.WAS_DERIVED_FROM;
        Map<String, StringBuilder> quickStatements = new HashMap<>();
        try {
            while (suggestions.hasNext()) {
                BindingSet suggestion = suggestions.next();
                Value datasetValue = suggestion.getValue("dataset");
                String currentDataset = datasetValue == null ? parameters.dataset : datasetValue.stringValue();
                String subject = suggestion.getValue("item").stringValue().substring(Utils.WIKIBASE_URIS.entity().length());
                Value mainPropertyValue = suggestion.getValue("property");
                String mainProperty = mainPropertyValue == null ? parameters.property : mainPropertyValue.stringValue().substring(Utils.WIKIBASE_URIS
                    .property(WikibaseUris.PropertyType.CLAIM).length());
                String statementUuid = suggestion.getValue("statement_node").stringValue().substring(Utils.WIKIBASE_URIS.statement().length());
                String statementProperty = suggestion.getValue("statement_property").stringValue();
                Value statementValue = suggestion.getValue("statement_value");
                String qsKey = statementUuid + "|" + currentDataset;
                log.debug("Current QuickStatement key from RDF statement node and dataset: {}", qsKey);
                // Check statement, qualifier, reference
                if (statementProperty.startsWith(Utils.WIKIBASE_URIS.property(WikibaseUris.PropertyType.STATEMENT))) {
                    StringBuilder quickStatement = quickStatements.getOrDefault(qsKey, new StringBuilder());
                    String statement = subject + "\t" + mainProperty + "\t" + Utils.rdfValueToQuickStatement(statementValue);
                    if (quickStatement.length() == 0)
                        log.debug("New key. Will start a new QuickStatement with statement: [{}]", statement);
                    else
                        log.debug("Existing key. Will update QuickStatement [{}] with statement [{}]", quickStatement, statement);
                    quickStatements.put(qsKey, quickStatement.insert(0, statement));
                } else if (statementProperty.startsWith(qualifierPrefix)) {
                    StringBuilder quickStatement = quickStatements.getOrDefault(qsKey, new StringBuilder());
                    String qualifier = "\t" + statementProperty.substring(qualifierPrefix.length()) + "\t" + Utils.rdfValueToQuickStatement(statementValue);
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
                            referenceProperty.substring(Utils.WIKIBASE_URIS.property(WikibaseUris.PropertyType.REFERENCE).length()).replace("P", "S") +
                            "\t" +
                            Utils.rdfValueToQuickStatement(referenceValue);
                    if (quickStatement.length() == 0)
                        log.debug("New key. Will start a new QuickStatement with reference: [{}]", reference);
                    else
                        log.debug("Existing key. Will update QuickStatement [{}] with reference [{}]", quickStatement, reference);
                    quickStatements.put(qsKey, quickStatement.append(reference));
                }
            }
        } catch (QueryEvaluationException qee) {
            log.error("Failed evaluating the search query. The stack trace follows.", qee);
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

    private class RequestParameters {
        private String dataset;
        private String property;
        private String value;
        private int offset;
        private int limit;

        @Override
        public String toString() {
            return String.format(
                Locale.ENGLISH,
                "dataset = %s; PID filter = %s; value filter = %s; SPARQL offset = %d; limit = %d",
                dataset, property, value, offset, limit);
        }
    }

}
