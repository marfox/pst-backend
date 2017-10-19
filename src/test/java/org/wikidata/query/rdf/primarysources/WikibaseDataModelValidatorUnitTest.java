package org.wikidata.query.rdf.primarysources;

import com.carrotsearch.randomizedtesting.RandomizedRunner;
import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.google.common.io.Resources;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.openrdf.model.*;
import org.openrdf.model.impl.TreeModel;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.util.Models;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.Rio;
import org.wikidata.query.rdf.common.uri.Provenance;
import org.wikidata.query.rdf.common.uri.WikibaseUris;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Locale;
import java.util.UUID;

/**
 * @author Marco Fossati - User:Hjfocs
 * @since 0.2.4
 * Created on Jun 19, 2017.
 */
@RunWith(RandomizedRunner.class)
public class WikibaseDataModelValidatorUnitTest extends RandomizedTest {

    private static final String GOOD_DATASET_FILE_NAME = "good_chuck_berry.ttl"; // Valid data model
    private static final String BAD_DATASET_FILE_NAME = "bad_chuck_berry.ttl"; // Invalid data model
    private static final String BAD_RDF_FILE_NAME = "just_bad_rdf.ttl"; // Invalid RDF
    private static final String BASE_URI = "http://test.dataset";
    private static final RDFFormat RDF_FORMAT = RDFFormat.TURTLE;

    private static WikibaseDataModelValidator validator;
    private static InputStream goodDataset;
    private static InputStream badDataset;
    private static Model goodParsedDataset;
    private static Model badParsedDataset;

    @BeforeClass
    public static void setUpOnce() throws RDFParseException, IOException {
        validator = new WikibaseDataModelValidator();
        try (InputStream is = openTestDatasetStream(GOOD_DATASET_FILE_NAME)) {
            goodDataset = is;
            goodParsedDataset = Rio.parse(goodDataset, BASE_URI, RDF_FORMAT);
        }
        try (InputStream is = openTestDatasetStream(BAD_DATASET_FILE_NAME)) {
            badDataset = is;
            badParsedDataset = Rio.parse(badDataset, BASE_URI, RDF_FORMAT);
        }
    }

    private static InputStream openTestDatasetStream(String fileName) throws IOException {
        return Resources.asByteSource(
                Resources.getResource(fileName))
                .openBufferedStream();
    }

    @Test
    public void testGoodSyntax() throws Exception {
        try (InputStream goodRDF = openTestDatasetStream(GOOD_DATASET_FILE_NAME)) {
            Model shouldBeGood = validator.checkSyntax(goodRDF, BASE_URI, RDF_FORMAT);
            assertNotNull(shouldBeGood);
            assertEquals(goodParsedDataset, shouldBeGood);
        }
    }

    @Test(expected = RDFParseException.class)
    public void testBadSyntax() throws Exception {
        try (InputStream badRDF = openTestDatasetStream(BAD_RDF_FILE_NAME)) {
            validator.checkSyntax(badRDF, BASE_URI, RDF_FORMAT);
        }
    }

    @Test
    public void testValidateItemTriple() throws Exception {
        ValueFactory vf = ValueFactoryImpl.getInstance();

        // Invalid triple components
        Resource invalidSubject = vf.createURI("http://not.a.wikidata.item");
        URI invalidProperty = vf.createURI("http://quite.invalid.wikidata.property");
        Value invalidObject = vf.createURI("http://im.no.reified.statement.node");

        // Valid triple components
        Resource validSubject = vf.createURI(WikibaseDataModelValidator.VALID_NAMESPACES.entity() + "Q666");
        URI validProperty = vf.createURI(WikibaseDataModelValidator.VALID_NAMESPACES.property(WikibaseUris.PropertyType.CLAIM) + "P88");
        Value validObject = vf.createURI(WikibaseDataModelValidator.VALID_NAMESPACES.statement() + "Q666-" + UUID.randomUUID().toString());

        // Combine valid and invalid components into a set of test triples
        Statement totallyInvalid = vf.createStatement(invalidSubject, invalidProperty, invalidObject);
        Statement totallyValid = vf.createStatement(validSubject, validProperty, validObject);
        Statement withInvalidSubject = vf.createStatement(invalidSubject, validProperty, validObject);
        Statement withInvalidProperty = vf.createStatement(validSubject, invalidProperty, validObject);
        Statement withInvalidObject = vf.createStatement(validSubject, validProperty, invalidObject);
        Statement withInvalidSubjectAndProperty = vf.createStatement(invalidSubject, invalidProperty, validObject);
        Statement withInvalidPropertyAndObject = vf.createStatement(validSubject, invalidProperty, invalidObject);
        Statement withInvalidSubjectAndObject = vf.createStatement(invalidSubject, validProperty, invalidObject);

        assertEquals(Arrays.asList(invalidSubject.stringValue(), invalidProperty.stringValue(), invalidObject.stringValue()),
                validator.validateItemTriple(totallyInvalid));
        assertEquals(new ArrayList<>(), validator.validateItemTriple(totallyValid));
        assertEquals(Arrays.asList(invalidSubject.stringValue()), validator.validateItemTriple(withInvalidSubject));
        assertEquals(Arrays.asList(invalidProperty.stringValue()), validator.validateItemTriple(withInvalidProperty));
        assertEquals(Arrays.asList(invalidObject.stringValue()), validator.validateItemTriple(withInvalidObject));
        assertEquals(Arrays.asList(invalidSubject.stringValue(), invalidProperty.stringValue()), validator.validateItemTriple(withInvalidSubjectAndProperty));
        assertEquals(Arrays.asList(invalidProperty.stringValue(), invalidObject.stringValue()), validator.validateItemTriple(withInvalidPropertyAndObject));
        assertEquals(Arrays.asList(invalidSubject.stringValue(), invalidObject.stringValue()), validator.validateItemTriple(withInvalidSubjectAndObject));
    }

    @Test
    public void testValidatePropertyTriple() throws Exception {
        ValueFactory vf = ValueFactoryImpl.getInstance();

        // Tricky invalid triple components
        Resource subjectWithInvalidUUID = vf.createURI(WikibaseDataModelValidator.VALID_NAMESPACES.statement() + "Q" + String.valueOf(randomIntBetween(1,
                100000)) +
                "-this-is-not-a-uuid");
        URI propertyWithInvalidNamespace = vf.createURI(WikibaseDataModelValidator.VALID_NAMESPACES.property(WikibaseUris.PropertyType.STATEMENT_VALUE) + "P"
                + String.valueOf(randomIntBetween(1, 100000)));
        Value objectWithInvalidNamespace = vf.createURI(WikibaseDataModelValidator.VALID_NAMESPACES.value() + "Q" + String.valueOf(randomIntBetween(1,
                100000)));
        Value objectWithTypo = vf.createURI("http://www.wikidata.orge/entiti/" + "Q" + String.valueOf(randomIntBetween(1, 100000)));
        Value unresolvableObject = vf.createURI("http://this.leads.to.nowhere");

        // Valid triple components
        Resource validSubject = vf.createURI(WikibaseDataModelValidator.VALID_NAMESPACES.statement() + "Q666-" + UUID.randomUUID().toString());
        URI validProperty = vf.createURI(WikibaseDataModelValidator.VALID_NAMESPACES.property(WikibaseUris.PropertyType.STATEMENT) + "P" + String.valueOf(
                randomIntBetween(1, 100000)));
        Value validObject = vf.createLiteral(randomFloat());

        // Combine valid and invalid components into a set of test triples
        Statement totallyInvalid = vf.createStatement(subjectWithInvalidUUID, propertyWithInvalidNamespace, objectWithInvalidNamespace);
        Statement totallyValid = vf.createStatement(validSubject, validProperty, validObject);
        Statement withInvalidSubject = vf.createStatement(subjectWithInvalidUUID, validProperty, validObject);
        Statement withInvalidProperty = vf.createStatement(validSubject, propertyWithInvalidNamespace, validObject);
        Statement withInvalidNamespaceObject = vf.createStatement(validSubject, validProperty, objectWithInvalidNamespace);
        Statement withTypoObject = vf.createStatement(validSubject, validProperty, objectWithTypo);
        Statement withUnresolvableObject = vf.createStatement(validSubject, validProperty, unresolvableObject);
        Statement withInvalidSubjectAndProperty = vf.createStatement(subjectWithInvalidUUID, propertyWithInvalidNamespace, validObject);
        Statement withInvalidPropertyAndObject = vf.createStatement(validSubject, propertyWithInvalidNamespace, objectWithInvalidNamespace);
        Statement withInvalidSubjectAndObject = vf.createStatement(subjectWithInvalidUUID, validProperty, objectWithInvalidNamespace);

        assertEquals(Arrays.asList(subjectWithInvalidUUID.stringValue(), propertyWithInvalidNamespace.stringValue(), objectWithInvalidNamespace.stringValue()),
                validator.validatePropertyTriple(totallyInvalid));
        assertEquals(new ArrayList<>(), validator.validatePropertyTriple(totallyValid));
        assertEquals(Arrays.asList(subjectWithInvalidUUID.stringValue()), validator.validatePropertyTriple(withInvalidSubject));
        assertEquals(Arrays.asList(propertyWithInvalidNamespace.stringValue()), validator.validatePropertyTriple(withInvalidProperty));
        assertEquals(Arrays.asList(objectWithInvalidNamespace.stringValue()), validator.validatePropertyTriple(withInvalidNamespaceObject));
        assertEquals(Arrays.asList(objectWithTypo.stringValue()), validator.validatePropertyTriple(withTypoObject));
        assertEquals(Arrays.asList(unresolvableObject.stringValue()), validator.validatePropertyTriple(withUnresolvableObject));
        assertEquals(Arrays.asList(subjectWithInvalidUUID.stringValue(), propertyWithInvalidNamespace.stringValue()), validator.validatePropertyTriple(
                withInvalidSubjectAndProperty));
        assertEquals(Arrays.asList(propertyWithInvalidNamespace.stringValue(), objectWithInvalidNamespace.stringValue()), validator.validatePropertyTriple(
                withInvalidPropertyAndObject));
        assertEquals(Arrays.asList(subjectWithInvalidUUID.stringValue(), objectWithInvalidNamespace.stringValue()), validator.validatePropertyTriple(
                withInvalidSubjectAndObject));
    }

    @Test
    public void testValidateReferenceTriple() throws Exception {
        ValueFactory vf = ValueFactoryImpl.getInstance();

        // Tricky invalid triple components
        Resource subjectWithInvalidUUID = vf.createURI(WikibaseDataModelValidator.VALID_NAMESPACES.statement() + "Q" + String.valueOf(randomIntBetween(1,
                100000)) +
                "-this-is-not-a-uuid");
        URI invalidProperty = vf.createURI(Provenance.NAMESPACE + "wasderivedfrom");
        Value objectWithInvalidHash = vf.createURI(WikibaseDataModelValidator.VALID_NAMESPACES.reference() + "IdontTh1nkImag00dHash");

        // Valid triple components
        Resource validSubject = vf.createURI(WikibaseDataModelValidator.VALID_NAMESPACES.statement() + "Q666-" + UUID.randomUUID().toString());
        URI validProperty = vf.createURI(Provenance.WAS_DERIVED_FROM);
        Value validObject = createValidReferenceNode(vf);

        // Combine valid and invalid components into a set of test triples
        Statement totallyInvalid = vf.createStatement(subjectWithInvalidUUID, invalidProperty, objectWithInvalidHash);
        Statement totallyValid = vf.createStatement(validSubject, validProperty, validObject);
        Statement withInvalidSubject = vf.createStatement(subjectWithInvalidUUID, validProperty, validObject);
        Statement withInvalidProperty = vf.createStatement(validSubject, invalidProperty, validObject);
        Statement withInvalidNamespaceObject = vf.createStatement(validSubject, validProperty, objectWithInvalidHash);
        Statement withInvalidSubjectAndProperty = vf.createStatement(subjectWithInvalidUUID, invalidProperty, validObject);
        Statement withInvalidPropertyAndObject = vf.createStatement(validSubject, invalidProperty, objectWithInvalidHash);
        Statement withInvalidSubjectAndObject = vf.createStatement(subjectWithInvalidUUID, validProperty, objectWithInvalidHash);

        assertEquals(Arrays.asList(subjectWithInvalidUUID.stringValue(), invalidProperty.stringValue(), objectWithInvalidHash.stringValue()),
                validator.validateReferenceTriple(totallyInvalid));
        assertEquals(new ArrayList<>(), validator.validateReferenceTriple(totallyValid));
        assertEquals(Arrays.asList(subjectWithInvalidUUID.stringValue()), validator.validateReferenceTriple(withInvalidSubject));
        assertEquals(Arrays.asList(invalidProperty.stringValue()), validator.validateReferenceTriple(withInvalidProperty));
        assertEquals(Arrays.asList(objectWithInvalidHash.stringValue()), validator.validateReferenceTriple(withInvalidNamespaceObject));
        assertEquals(Arrays.asList(subjectWithInvalidUUID.stringValue(), invalidProperty.stringValue()), validator.validateReferenceTriple(
                withInvalidSubjectAndProperty));
        assertEquals(Arrays.asList(invalidProperty.stringValue(), objectWithInvalidHash.stringValue()), validator.validateReferenceTriple(
                withInvalidPropertyAndObject));
        assertEquals(Arrays.asList(subjectWithInvalidUUID.stringValue(), objectWithInvalidHash.stringValue()), validator.validateReferenceTriple(
                withInvalidSubjectAndObject));

    }

    @Test
    public void testValidateQualifierTriple() throws Exception {
        ValueFactory vf = ValueFactoryImpl.getInstance();

        // Tricky invalid triple components
        Resource subjectWithInvalidUUID = vf.createURI(WikibaseDataModelValidator.VALID_NAMESPACES.statement() + "Q" + String.valueOf(randomIntBetween(1,
                100000)) +
                "-this-is-not-a-uuid");
        URI propertyWithInvalidNamespace = vf.createURI(WikibaseDataModelValidator.VALID_NAMESPACES.property(WikibaseUris.PropertyType.QUALIFIER_VALUE) + "P"
                + String.valueOf(randomIntBetween(1, 100000)));
        Value objectWithInvalidNamespace = vf.createURI(WikibaseDataModelValidator.VALID_NAMESPACES.reference() + "Q" + String.valueOf(randomIntBetween(1,
                100000)));

        // Valid triple components
        Resource validSubject = vf.createURI(WikibaseDataModelValidator.VALID_NAMESPACES.statement() + "Q666-" + UUID.randomUUID().toString());
        URI validProperty = vf.createURI(WikibaseDataModelValidator.VALID_NAMESPACES.property(WikibaseUris.PropertyType.QUALIFIER) + "P" + String.valueOf(
                randomIntBetween(1, 100000)));
        Value validObject = vf.createURI(WikibaseDataModelValidator.VALID_NAMESPACES.entity() + "Q" + String.valueOf(randomIntBetween(1, 100000)));

        // Combine valid and invalid components into a set of test triples
        Statement totallyInvalid = vf.createStatement(subjectWithInvalidUUID, propertyWithInvalidNamespace, objectWithInvalidNamespace);
        Statement totallyValid = vf.createStatement(validSubject, validProperty, validObject);
        Statement withInvalidSubject = vf.createStatement(subjectWithInvalidUUID, validProperty, validObject);
        Statement withInvalidProperty = vf.createStatement(validSubject, propertyWithInvalidNamespace, validObject);
        Statement withInvalidNamespaceObject = vf.createStatement(validSubject, validProperty, objectWithInvalidNamespace);
        Statement withInvalidSubjectAndProperty = vf.createStatement(subjectWithInvalidUUID, propertyWithInvalidNamespace, validObject);
        Statement withInvalidPropertyAndObject = vf.createStatement(validSubject, propertyWithInvalidNamespace, objectWithInvalidNamespace);
        Statement withInvalidSubjectAndObject = vf.createStatement(subjectWithInvalidUUID, validProperty, objectWithInvalidNamespace);

        assertEquals(Arrays.asList(subjectWithInvalidUUID.stringValue(), propertyWithInvalidNamespace.stringValue(), objectWithInvalidNamespace.stringValue()),
                validator.validateQualifierTriple(totallyInvalid));
        assertEquals(new ArrayList<>(), validator.validateQualifierTriple(totallyValid));
        assertEquals(Arrays.asList(subjectWithInvalidUUID.stringValue()), validator.validateQualifierTriple(withInvalidSubject));
        assertEquals(Arrays.asList(propertyWithInvalidNamespace.stringValue()), validator.validateQualifierTriple(withInvalidProperty));
        assertEquals(Arrays.asList(objectWithInvalidNamespace.stringValue()), validator.validateQualifierTriple(withInvalidNamespaceObject));
        assertEquals(Arrays.asList(subjectWithInvalidUUID.stringValue(), propertyWithInvalidNamespace.stringValue()), validator.validateQualifierTriple(
                withInvalidSubjectAndProperty));
        assertEquals(Arrays.asList(propertyWithInvalidNamespace.stringValue(), objectWithInvalidNamespace.stringValue()), validator.validateQualifierTriple(
                withInvalidPropertyAndObject));
        assertEquals(Arrays.asList(subjectWithInvalidUUID.stringValue(), objectWithInvalidNamespace.stringValue()), validator.validateQualifierTriple(
                withInvalidSubjectAndObject));
    }

    @Test
    public void testValidateReferenceValueTriple() throws Exception {
        ValueFactory vf = ValueFactoryImpl.getInstance();

        // Tricky invalid triple components
        Resource subjectWithInvalidHash = vf.createURI(WikibaseDataModelValidator.VALID_NAMESPACES.reference() + "IdontTh1nkImag00dHash");
        URI propertyWithInvalidNamespace = vf.createURI(WikibaseDataModelValidator.VALID_NAMESPACES.property(WikibaseUris.PropertyType.REFERENCE_VALUE) + "P"
                + String.valueOf(randomIntBetween(1, 100000)));
        Value objectWithInvalidNamespace = vf.createURI(WikibaseDataModelValidator.VALID_NAMESPACES.entityData() + "Q" + String.valueOf(randomIntBetween(1,
                100000)));
        Value objectWithTypo = vf.createURI("http://wwww.wikidata.org/entit/" + "Q" + String.valueOf(randomIntBetween(1, 100000)));
        Value unresolvableObject = vf.createURI("http://road.to.nowhere");

        // Valid triple components
        Resource validSubject = createValidReferenceNode(vf);
        URI validProperty = vf.createURI(WikibaseDataModelValidator.VALID_NAMESPACES.property(WikibaseUris.PropertyType.REFERENCE) + "P" + String.valueOf(
                randomIntBetween(1, 100000)));
        Value validObject = vf.createURI("https://en.wikipedia.org");

        // Combine valid and invalid components into a set of test triples
        Statement totallyInvalid = vf.createStatement(subjectWithInvalidHash, propertyWithInvalidNamespace, objectWithInvalidNamespace);
        Statement totallyValid = vf.createStatement(validSubject, validProperty, validObject);
        Statement withInvalidSubject = vf.createStatement(subjectWithInvalidHash, validProperty, validObject);
        Statement withInvalidProperty = vf.createStatement(validSubject, propertyWithInvalidNamespace, validObject);
        Statement withInvalidNamespaceObject = vf.createStatement(validSubject, validProperty, objectWithInvalidNamespace);
        Statement withTypoObject = vf.createStatement(validSubject, validProperty, objectWithTypo);
        Statement withUnresolvableObject = vf.createStatement(validSubject, validProperty, unresolvableObject);
        Statement withInvalidSubjectAndProperty = vf.createStatement(subjectWithInvalidHash, propertyWithInvalidNamespace, validObject);
        Statement withInvalidPropertyAndObject = vf.createStatement(validSubject, propertyWithInvalidNamespace, objectWithInvalidNamespace);
        Statement withInvalidSubjectAndObject = vf.createStatement(subjectWithInvalidHash, validProperty, objectWithInvalidNamespace);

        assertEquals(Arrays.asList(subjectWithInvalidHash.stringValue(), propertyWithInvalidNamespace.stringValue(), objectWithInvalidNamespace.stringValue()),
                validator.validateReferenceValueTriple(totallyInvalid));
        assertEquals(new ArrayList<>(), validator.validateReferenceValueTriple(totallyValid));
        assertEquals(Arrays.asList(subjectWithInvalidHash.stringValue()), validator.validateReferenceValueTriple(withInvalidSubject));
        assertEquals(Arrays.asList(propertyWithInvalidNamespace.stringValue()), validator.validateReferenceValueTriple(withInvalidProperty));
        assertEquals(Arrays.asList(objectWithInvalidNamespace.stringValue()), validator.validateReferenceValueTriple(withInvalidNamespaceObject));
        assertEquals(Arrays.asList(objectWithTypo.stringValue()), validator.validateReferenceValueTriple(withTypoObject));
        assertEquals(Arrays.asList(unresolvableObject.stringValue()), validator.validateReferenceValueTriple(withUnresolvableObject));
        assertEquals(Arrays.asList(subjectWithInvalidHash.stringValue(), propertyWithInvalidNamespace.stringValue()), validator.validateReferenceValueTriple(
                withInvalidSubjectAndProperty));
        assertEquals(Arrays.asList(propertyWithInvalidNamespace.stringValue(), objectWithInvalidNamespace.stringValue()), validator
                .validateReferenceValueTriple(
                        withInvalidPropertyAndObject));
        assertEquals(Arrays.asList(subjectWithInvalidHash.stringValue(), objectWithInvalidNamespace.stringValue()), validator.validateReferenceValueTriple(
                withInvalidSubjectAndObject));
    }

    @Test
    public void testHandleDataset() throws Exception {
        Model good = validator.handleDataset(goodParsedDataset).getKey();
        Model bad = validator.handleDataset(badParsedDataset).getKey();
        assertEquals(goodParsedDataset, good);
        assertTrue(Models.isSubset(bad, badParsedDataset));
        assertEquals(new TreeModel(), bad);
    }

    /*
     * Build a valid reference node with a SHA-1 hash
     */
    private URI createValidReferenceNode(ValueFactory vf) throws NoSuchAlgorithmException, UnsupportedEncodingException {
        MessageDigest md = MessageDigest.getInstance("SHA-1");
        String toDigest = "I wanna become a SHA-1 hash";
        byte[] digest = md.digest(toDigest.getBytes("utf8"));
        StringBuilder hash = new StringBuilder();
        for (byte b : digest) {
            hash.append(String.format(Locale.ENGLISH, "%02x", b & 0xff));
        }
        return vf.createURI(WikibaseDataModelValidator.VALID_NAMESPACES.reference() + hash.toString());
    }
}
