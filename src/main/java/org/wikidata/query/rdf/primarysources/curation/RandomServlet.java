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

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.wikidata.query.rdf.primarysources.curation.SuggestServlet.*;

/**
 * @author Marco Fossati - User:Hjfocs
 * @since 0.2.5
 * Created on Dec 05, 2017.
 */
public class RandomServlet extends HttpServlet {
    private static final String ONE_DATASET_SUBJECT_LIST_QUERY =
            "SELECT DISTINCT ?subject " +
                    "WHERE {" +
                    "  GRAPH <" + DATASET_PLACE_HOLDER + "> {" +
                    "    ?subject ?property ?value ." +
                    "    FILTER STRSTARTS(str(?subject), \"http://www.wikidata.org/entity/Q\") ." +
                    "  }" +
                    "}";
    private static final String ALL_DATASETS_SUBJECT_LIST_QUERY =
            "SELECT DISTINCT ?subject " +
                    "WHERE {" +
                    "  GRAPH ?dataset {" +
                    "    ?subject ?property ?value ." +
                    "    FILTER STRSTARTS(str(?subject), \"http://www.wikidata.org/entity/Q\") ." +
                    "  }" +
                    "  FILTER STRENDS(str(?dataset), \"new\") ." +
                    "}";
    private static final Logger log = LoggerFactory.getLogger(RandomServlet.class);
    private String dataset;
    private String qId;

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        boolean ok = processRequest(request, response);
        if (!ok) return;
        List<String> items = getSubjectList();
        if (items == null) {
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Something went wrong when retrieving the list of subject items.");
            return;
        }
        qId = pickRandomItem(items);
        TupleQueryResult suggestions = getSuggestions();
        sendResponse(response, suggestions);
    }

    private List<String> getSubjectList() throws IOException {
        List<String> subjectList = new ArrayList<>();
        String query = dataset.equals("all") ? ALL_DATASETS_SUBJECT_LIST_QUERY : ONE_DATASET_SUBJECT_LIST_QUERY.replace(DATASET_PLACE_HOLDER, dataset);
        TupleQueryResult results = runSparqlQuery(query);
        try {
            while (results.hasNext()) {
                BindingSet result = results.next();
                String subject = result.getValue("subject").stringValue();
                subjectList.add(subject.substring(WIKIBASE_URIS.entity().length()));
            }
        } catch (QueryEvaluationException qee) {
            log.error("Failed evaluating the suggestion query: {}", qee.getMessage());
            return null;
        }
        return subjectList;
    }

    private String pickRandomItem(List<String> items) {
        Random random = new Random();
        int randomIndex = random.nextInt(items.size());
        return items.get(randomIndex);
    }

    private TupleQueryResult getSuggestions() throws IOException {
        String query = dataset.equals("all") ? ALL_DATASETS_QUERY.replace(QID_PLACE_HOLDER, qId) : ONE_DATASET_QUERY.replace(QID_PLACE_HOLDER, qId).replace(DATASET_PLACE_HOLDER, dataset);
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

    private boolean processRequest(HttpServletRequest request, HttpServletResponse response) throws IOException {
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
}
