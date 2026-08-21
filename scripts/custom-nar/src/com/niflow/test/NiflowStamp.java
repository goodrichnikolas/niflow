package com.niflow.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;

/**
 * A processor that exists in no harvested catalog — on purpose.
 *
 * <p>niflow's rulebooks are harvested from stock NiFi containers, so every
 * type they know about is a type Apache ships. Work runs custom NARs, and the
 * question "what does niflow do with a type it has never seen" could only be
 * answered by hand-waving until this existed. It is deliberately shaped to hit
 * the interesting paths: a required property with a default, a *sensitive*
 * property (which NiFi never reads back), a non-default relationship set, and
 * a dynamic-property-friendly surface.
 *
 * <p>Only depends on nifi-api, so it compiles with javac and a single jar —
 * see build.sh. It is a test fixture, not an example of a good processor.
 */
@Tags({"niflow", "test", "custom"})
@CapabilityDescription("Stamps an attribute. Exists only in niflow's test NAR.")
public class NiflowStamp extends AbstractProcessor {

    public static final PropertyDescriptor STAMP = new PropertyDescriptor.Builder()
            .name("Stamp Value")
            .description("Value written to the niflow.stamp attribute.")
            .required(true)
            .defaultValue("stamped")
            .addValidator(Validator.VALID)
            .build();

    public static final PropertyDescriptor SECRET = new PropertyDescriptor.Builder()
            .name("Stamp Secret")
            .description("A sensitive property, so a plan has one it cannot read back.")
            .required(false)
            .sensitive(true)
            .addValidator(Validator.VALID)
            .build();

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("success").description("Stamped FlowFiles.").build();

    public static final Relationship FAILURE = new Relationship.Builder()
            .name("failure").description("Never used; it exists to be wired.").build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return new ArrayList<>(Arrays.asList(STAMP, SECRET));
    }

    @Override
    public Set<Relationship> getRelationships() {
        return new HashSet<>(Arrays.asList(SUCCESS, FAILURE));
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String name) {
        return new PropertyDescriptor.Builder()
                .name(name)
                .required(false)
                .dynamic(true)
                .addValidator(Validator.VALID)
                .build();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session)
            throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        flowFile = session.putAttribute(flowFile, "niflow.stamp",
                context.getProperty(STAMP).getValue());
        session.transfer(flowFile, SUCCESS);
    }
}
