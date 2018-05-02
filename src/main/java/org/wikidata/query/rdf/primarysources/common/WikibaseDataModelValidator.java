package org.wikidata.query.rdf.primarysources.common;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.http.client.fluent.Request;
import org.openrdf.model.Model;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.TreeModel;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.Rio;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.query.rdf.common.uri.Provenance;
import org.wikidata.query.rdf.common.uri.WikibaseUris;

import com.google.common.collect.ImmutableMap;

/**
 * Validate a given dataset against the <a href="https://www.mediawiki.org/wiki/Wikibase/Indexing/RDF_Dump_Format#Data_model">Wikidata RDF data model</a>.
 * The dataset undergoes RDF syntax check first, then the actual data model validation.
 *
 * @author Marco Fossati - <a href="https://meta.wikimedia.org/wiki/User:Hjfocs">User:Hjfocs</a>
 * @since 0.2.5 - created on Jun 19, 2017.
 */
public class WikibaseDataModelValidator {

    /**
     * The set of Wikidata namespaces.
     */
    static final WikibaseUris VALID_NAMESPACES = WikibaseUris.WIKIDATA;
    /**
     * Map of regular expressions that validate the following Wikidata terms:
     * <ul>
     * <li>item, e.g., <code>Q9521</code>;</li>
     * <li>property, e.g., <code>P18</code>;</li>
     * <li>reified statement, e.g., <code>Q5921-583C7277-B344-4C96-8CF2-0557C2D0CD34</code>;</li>
     * <li>reified reference, e.g., <code>288ab581e7d2d02995a26dfa8b091d96e78457fc</code>.</li>
     * </ul>
     */
    private static final Map<String, Pattern> TERM_VALIDATORS = ImmutableMap.of(
        "item", Pattern.compile("^Q\\d+$"),
        "property", Pattern.compile("^P\\d+$"),
        "statement", Pattern.compile("^Q\\d+-[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"),
        "reference", Pattern.compile("^[0-9a-f]{40}$"));
    /**
     * 3 characters is the maximum value to consider a given resource as invalid due to a typo.
     */
    private static final int EDIT_DISTANCE_THRESHOLD = 3;
    /**
     * Timeout in milliseconds when trying to resolve a given URL, see {@link #validateURL(String)}.
     */
    private static final int RESOLVE_URL_TIMEOUT = 1000;
    private static final Logger log = LoggerFactory.getLogger(WikibaseDataModelValidator.class);

    /**
     * Check the RDF syntax correctness of a given dataset.
     * Note that parsing is done in memory over the whole dataset.
     *
     * @param dataset the input stream of the dataset to check.
     * @param baseURI the base URI.
     * @param format  the RDF format used to serialize the input dataset.
     * @return the successfully parsed RDF {@link Model}.
     * @throws IOException       if there are troubles reading the input stream.
     * @throws RDFParseException if the dataset is not valid RDF.
     */
    public Model checkSyntax(InputStream dataset, String baseURI, RDFFormat format) throws IOException, RDFParseException {
        return Rio.parse(dataset, baseURI, format);
    }

    /**
     * Validate the given dataset, remove invalid triples, and log the list of invalid components.
     *
     * @param dataset the RDF dataset to be validated, which has already undergone syntax check.
     * @return a subset of the input dataset, together with the list of pruned invalid triples.
     */
    public AbstractMap.SimpleImmutableEntry<Model, List<String>> handleDataset(Model dataset) {
        Model valid = new TreeModel();
        List<String> invalid = new ArrayList<>();
        for (Statement statement : dataset) {
            handleSubject(valid, invalid, statement);
        }
        if (invalid.isEmpty()) {
            log.info("Your dataset is valid and will be fully uploaded");
        } else {
            log.warn("Your dataset has issues, only valid triples will be uploaded. List of invalid triples: {}", invalid);
        }
        return new AbstractMap.SimpleImmutableEntry<>(valid, invalid);
    }

    /**
     * Validate a RDF triple with a Wikidata Item as subject.
     * Example:
     * Chuck Berry  has image   reified node
     * wd:Q5921     p:P18       wds:Q5921-{uuid}
     * Taken from https://www.wikidata.org/wiki/Special:EntityData/Q5921.ttl
     *
     * @param itemTriple the Item triple to be validated
     * @return a {@link List} of invalid triple components or empty if everything is valid
     */
    List<String> validateItemTriple(Statement itemTriple) {
        List<String> invalid = new ArrayList<>();
        String subject = itemTriple.getSubject().stringValue();
        if (isInvalidTripleComponent(subject, VALID_NAMESPACES.entity(), "item")) {
            invalid.add(subject);
        }
        String predicate = itemTriple.getPredicate().stringValue();
        if (isInvalidTripleComponent(predicate, VALID_NAMESPACES.property(WikibaseUris.PropertyType.CLAIM), "property")) {
            invalid.add(predicate);
        }
        String object = itemTriple.getObject().stringValue();
        if (isInvalidTripleComponent(object, VALID_NAMESPACES.statement(), "statement")) {
            invalid.add(object);
        }
        return invalid;
    }

    /**
     * Validate a RDF triple with a reified statement as subject.
     * Example:
     * reified node      has image   Commons URL
     * wds:Q5921-{uuid}  ps:P18      <http://commons.wikimedia.org/wiki/Special:FilePath/Chuck-berry-2007-07-18.jpg>
     * Taken from https://www.wikidata.org/wiki/Special:EntityData/Q5921.ttl
     *
     * @param propertyTriple the Item triple to be validated
     * @return a {@link List} of invalid triple components or empty if everything is valid
     */
    List<String> validatePropertyTriple(Statement propertyTriple) {
        List<String> invalid = new ArrayList<>();
        String subject = propertyTriple.getSubject().stringValue();
        if (isInvalidTripleComponent(subject, VALID_NAMESPACES.statement(), "statement")) {
            invalid.add(subject);
        }
        String predicate = propertyTriple.getPredicate().stringValue();
        if (isInvalidTripleComponent(predicate, VALID_NAMESPACES.property(WikibaseUris.PropertyType.STATEMENT), "property")) {
            invalid.add(predicate);
        }
        // The object can be a a literal, an Item, or a URL
        Value object = propertyTriple.getObject();
        if (object instanceof URI) {
            String objectString = object.stringValue();
            if (isInvalidTripleComponent(objectString, VALID_NAMESPACES.entity(), "item")) {
                // Not an Item, if it starts with "http://www.wikidata.org/", it probably means that an invalid Wikidata resource is used
                if (objectString.startsWith(VALID_NAMESPACES.root() + "/")) {
                    log.error("Probably a Wikidata term, but not an Item: {}", objectString);
                    invalid.add(objectString);
                } else {
                    // Check if it's a typo via edit distance between the current namespace and the valid one
                    int distance = computeNamespaceDistance(objectString, VALID_NAMESPACES.entity());
                    if (distance <= EDIT_DISTANCE_THRESHOLD) {
                        log.error("Probably a typo: {}", objectString);
                        invalid.add(objectString);
                    }
                }
            }
        }
        return invalid;
    }

    /**
     * Validate a RDF triple that involves a reference.
     * Example:
     * reified node     has reference           reified reference
     * wds:Qn-uuid      prov:wasDerivedFrom     wdref:{hash}
     * Taken from https://www.wikidata.org/wiki/Special:EntityData/Q5921.ttl
     *
     * @param referenceTriple the reference triple to be validated
     * @return a {@link List} of invalid triple components or empty if everything is valid
     */
    List<String> validateReferenceTriple(Statement referenceTriple) {
        List<String> invalid = new ArrayList<>();
        String subject = referenceTriple.getSubject().stringValue();
        if (isInvalidTripleComponent(subject, VALID_NAMESPACES.statement(), "statement")) {
            invalid.add(subject);
        }
        String predicate = referenceTriple.getPredicate().stringValue();
        if (!predicate.equals(Provenance.WAS_DERIVED_FROM)) {
            invalid.add(predicate);
        }
        String object = referenceTriple.getObject().stringValue();
        if (isInvalidTripleComponent(object, VALID_NAMESPACES.reference(), "reference")) {
            invalid.add(object);
        }
        return invalid;
    }

    /**
     * Validate a RDF triple that involves a qualifier.
     * Example:
     * reified node     has media legend    literal
     * wds:Q666-{uuid}  pq:P2096            "Chuck Berry (2007)"@ca
     * Taken from https://www.wikidata.org/wiki/Special:EntityData/Q5921.ttl
     *
     * @param qualifierTriple the qualifier triple to be validated
     * @return a {@link List} of invalid triple components or empty if everything is valid
     */
    List<String> validateQualifierTriple(Statement qualifierTriple) {
        List<String> invalid = new ArrayList<>();
        String subject = qualifierTriple.getSubject().stringValue();
        if (isInvalidTripleComponent(subject, VALID_NAMESPACES.statement(), "statement")) {
            invalid.add(subject);
        }
        String predicate = qualifierTriple.getPredicate().stringValue();
        if (isInvalidTripleComponent(predicate, VALID_NAMESPACES.property(WikibaseUris.PropertyType.QUALIFIER), "property")) {
            invalid.add(predicate);
        }
        // The object can be a a literal or an Item
        Value object = qualifierTriple.getObject();
        if (object instanceof URI) {
            String objectString = object.stringValue();
            if (isInvalidTripleComponent(objectString, VALID_NAMESPACES.entity(), "item")) {
                invalid.add(objectString);
            }
        }
        return invalid;
    }

    /**
     * Validate a RDF triple that contains the reference value.
     * Example:
     * reified reference    was imported from   Russian Wikipedia
     * wdref:{hash}         pr:P143             wd:Q206855
     *
     * @param referenceValueTriple the reference value triple to be validated
     * @return a {@link List} of invalid triple components or empty if everything is valid
     */
    List<String> validateReferenceValueTriple(Statement referenceValueTriple) {
        List<String> invalid = new ArrayList<>();
        String subject = referenceValueTriple.getSubject().stringValue();
        if (isInvalidTripleComponent(subject, VALID_NAMESPACES.reference(), "reference")) {
            invalid.add(subject);
        }
        String predicate = referenceValueTriple.getPredicate().stringValue();
        if (isInvalidTripleComponent(predicate, VALID_NAMESPACES.property(WikibaseUris.PropertyType.REFERENCE), "property")) {
            invalid.add(predicate);
        }
        // The object can be an Item, or a URL
        String object = referenceValueTriple.getObject().stringValue();
        if (isInvalidTripleComponent(object, VALID_NAMESPACES.entity(), "item")) {
            // Not an Item, if it starts with "http://www.wikidata.org/", it probably means that an invalid Wikidata resource is used
            if (object.startsWith(VALID_NAMESPACES.root() + "/")) {
                log.error("Probably a Wikidata term, but not an Item: {}", object);
                invalid.add(object);
            } else {
                // Check if it's a typo via edit distance between the current namespace and the valid one
                int distance = computeNamespaceDistance(object, VALID_NAMESPACES.entity());
                if (distance <= EDIT_DISTANCE_THRESHOLD) {
                    log.error("Probably a typo: {}", object);
                    invalid.add(object);
                }
            }
        }
        return invalid;
    }

    /**
     * Dispatch the validation based on the triple subject.
     */
    private void handleSubject(Model valid, List<String> invalid, Statement statement) {
        List<String> currentInvalid;
        String subject = statement.getSubject().stringValue();
        if (subject.startsWith(WikibaseDataModelValidator.VALID_NAMESPACES.entity() + "Q")) {
            currentInvalid = validateItemTriple(statement);
            if (currentInvalid.isEmpty()) {
                valid.add(statement);
            } else {
                invalid.addAll(currentInvalid);
            }
        } else if (subject.startsWith(WikibaseDataModelValidator.VALID_NAMESPACES.statement())) {
            handlePredicate(valid, invalid, statement);
        } else if (subject.startsWith(WikibaseDataModelValidator.VALID_NAMESPACES.reference())) {
            currentInvalid = validateReferenceValueTriple(statement);
            if (currentInvalid.isEmpty()) {
                valid.add(statement);
            } else {
                invalid.addAll(currentInvalid);
            }
        } else {
            log.error("Invalid triple: {}", statement);
            invalid.add(statement.toString());
        }
    }

    /**
     * Dispatch the validation based on the triple predicate.
     */
    private void handlePredicate(Model valid, List<String> invalid, Statement statement) {
        List<String> currentInvalid;
        String predicate = statement.getPredicate().stringValue();
        if (predicate.startsWith(WikibaseDataModelValidator.VALID_NAMESPACES.property(WikibaseUris.PropertyType.STATEMENT))) {
            currentInvalid = validatePropertyTriple(statement);
            if (currentInvalid.isEmpty()) {
                valid.add(statement);
            } else {
                invalid.addAll(currentInvalid);
            }
        } else if (predicate.equals(Provenance.WAS_DERIVED_FROM)) {
            currentInvalid = validateReferenceTriple(statement);
            if (currentInvalid.isEmpty()) {
                valid.add(statement);
            } else {
                invalid.addAll(currentInvalid);
            }
        } else if (predicate.startsWith(WikibaseDataModelValidator.VALID_NAMESPACES.property(WikibaseUris.PropertyType.QUALIFIER))) {
            currentInvalid = validateQualifierTriple(statement);
            if (currentInvalid.isEmpty()) {
                valid.add(statement);
            } else {
                invalid.addAll(currentInvalid);
            }
        } else {
            log.error("Invalid triple: {}", statement);
            invalid.add(statement.toString());
        }
    }

    /**
     * Perform an HTTP GET to check that the given resource is resolvable in the Internet.
     *
     * @deprecated unreliable, it heavily impacts the validation time
     */
    private boolean validateURL(String resource) {
        java.net.URI uri;
        try {
            uri = new java.net.URI(resource);
        } catch (URISyntaxException use) {
            log.error("Malformed URI: {}. Reason: {}", resource, use.getLocalizedMessage());
            return false;
        }
        int status;
        try {
            status = Request.Get(uri)
                .connectTimeout(RESOLVE_URL_TIMEOUT)
                .execute()
                .returnResponse().getStatusLine().getStatusCode();
        } catch (IOException ioe) {
            log.error("Not resolvable: {}. Reason: {}", resource, ioe.getClass().getSimpleName());
            return false;
        }
        // Allow success (2xx) and redirection (3xx) code ranges
        if (status >= 400) {
            log.error("Not resolvable: {}. Reason: HTTP error code {}", resource, status);
            return false;
        }
        return true;
    }

    /**
     * Validate the given triple component.
     */
    private boolean isInvalidTripleComponent(String tripleComponent, String expectedNamespace, String expectedTerm) {
        if (tripleComponent.startsWith(expectedNamespace)) {
            String term = tripleComponent.substring(expectedNamespace.length());
            return !isValidTerm(term, expectedTerm);
        } else {
            return true;
        }
    }

    /**
     * Validate the given resource term.
     *
     * @param term             the resource term to validate.
     * @param expectedTermType one of <i>item</i>, <i>property</i>, <i>statement</i>, <i>reference</i>.
     * @return <i>true</i> if the term is valid, <i>false</i> otherwise.
     */
    public boolean isValidTerm(String term, String expectedTermType) {
        Pattern regex = TERM_VALIDATORS.get(expectedTermType);
        Matcher matcher = regex.matcher(term);
        return matcher.matches();
    }

    /**
     * The edit distance of a given resource is computed between its namespace and a valid one.
     * Note that the term is not involved here.
     */
    private int computeNamespaceDistance(String resource, String expectedNamespace) {
        Pattern pattern = TERM_VALIDATORS.get("item");
        // Don't match start-of-line + end-of-line
        String regex = pattern.pattern().replace("^", "").replace("$", "");
        String[] split = resource.split(regex);
        String namespace = split[0];
        return levenshteinDistance(namespace, expectedNamespace);
    }

    /**
     * Levenshtein distance implementation.
     * Copied from https://en.wikibooks.org/wiki/Algorithm_Implementation/Strings/Levenshtein_distance#Java
     * Rephrased to fit the code style.
     */
    private int levenshteinDistance(CharSequence lhs, CharSequence rhs) {
        int len0 = lhs.length() + 1;
        int len1 = rhs.length() + 1;
        // The array of distances
        int[] cost = new int[len0];
        int[] newcost = new int[len0];
        // Initial cost of skipping prefix in String s0
        for (int i = 0; i < len0; i++) cost[i] = i;
        // Dynamically computing the array of distances
        // Transformation cost for each letter in s1
        for (int j = 1; j < len1; j++) {
            // Initial cost of skipping prefix in String s1
            newcost[0] = j;
            // Transformation cost for each letter in s0
            for (int i = 1; i < len0; i++) {
                // Matching current letters in both strings
                int match = (lhs.charAt(i - 1) == rhs.charAt(j - 1)) ? 0 : 1;
                // Computing cost for each transformation
                int costReplace = cost[i - 1] + match;
                int costInsert = cost[i] + 1;
                int costDelete = newcost[i - 1] + 1;
                // Keep minimum cost
                newcost[i] = Math.min(Math.min(costInsert, costDelete), costReplace);
            }
            // Swap cost/newcost arrays
            int[] swap = cost;
            cost = newcost;
            newcost = swap;
        }
        // The distance is the cost for transforming all letters in both strings
        return cost[len0 - 1];
    }

}
