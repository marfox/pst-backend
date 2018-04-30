package org.wikidata.query.rdf.primarysources.common;

import org.wikidata.query.rdf.common.uri.WikibaseUris;
import org.wikidata.query.rdf.primarysources.ingestion.UploadServlet;

/**
 * A set of RDF namespaces and URIs used by the Wikidata primary sources tool.
 *
 * @author Marco Fossati - <a href="https://meta.wikimedia.org/wiki/User:Hjfocs">User:Hjfocs</a>
 * @since 0.2.5 - created on Apr 17, 2018.
 */
public final class RdfVocabulary {

    /**
     * The data provider should not care about the base URI. A constant is used instead.
     */
    public static final String BASE_URI = WikibaseUris.WIKIDATA.root();
    /**
     * URI prefix for users. Append the user name to build a full user URI.
     */
    public static final String USER_URI_PREFIX = BASE_URI + "/wiki/User:";
    /**
     * Namespace for metadata quads. Used to store data providers and users activities.
     * See private method {@code addMetadataQuads} in {@link UploadServlet} and
     * {@link org.wikidata.query.rdf.primarysources.curation.CurateServlet}.
     */
    public static final String METADATA_NAMESPACE = BASE_URI + "/primary-sources";
    /**
     * Predicate URI to couple a dataset with its uploader.
     */
    public static final String UPLOADED_BY_PREDICATE = METADATA_NAMESPACE + "/uploadedBy";
    /**
     * Predicate URI to couple a dataset with its description.
     */
    public static final String DESCRIPTION_PREDICATE = METADATA_NAMESPACE + "/description";

    private RdfVocabulary() {
    }
}
