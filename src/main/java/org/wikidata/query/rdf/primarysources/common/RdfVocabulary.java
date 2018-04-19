package org.wikidata.query.rdf.primarysources.common;

import javax.servlet.http.HttpServletResponse;

import org.wikidata.query.rdf.common.uri.WikibaseUris;
import org.wikidata.query.rdf.primarysources.ingestion.UploadServlet;

/**
 * @author Marco Fossati - User:Hjfocs
 * @since 0.2.5
 * Created on Apr 17, 2018.
 */
public final class RdfVocabulary {

    /**
     * The data provider should not care about the base URI. A constant is used instead.
     */
    public static final String BASE_URI = WikibaseUris.WIKIDATA.root();
    /**
     * Prefix URI for users. Append the user name to build a full user URI.
     */
    public static final String USER_URI_PREFIX = BASE_URI + "/wiki/User:";
    /**
     * Namespace URI for metadata triples. Used to store data providers and users activities.
     * See {@link UploadServlet#addMetadataQuads(UploadServlet.RequestParameters, HttpServletResponse)} and
     * {@link org.wikidata.query.rdf.primarysources.curation.CurateServlet}.
     */
    public static final String METADATA_NAMESPACE = BASE_URI + "/primary-sources";
    public static final String UPLOADED_BY_PREDICATE = METADATA_NAMESPACE + "/uploadedBy";
    public static final String DESCRIPTION_PREDICATE = METADATA_NAMESPACE + "/description";

    private RdfVocabulary() {
    }
}
