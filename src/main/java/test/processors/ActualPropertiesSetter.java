package test.processors;

import com.hazelcast.map.AbstractEntryProcessor;
import test.model.ContainerState;
import test.model.PositionState;
import test.model.Property;

import java.util.Date;
import java.util.Map;

public class ActualPropertiesSetter extends AbstractEntryProcessor<String, ContainerState> {

    private static final long serialVersionUID = -4101277772496491106L;

    private String transactionId;
    private Date time;
    private Integer endpoint;
    private final Map<String, String> changes;

    public ActualPropertiesSetter(final Date time, final String transactionId,
                                  final Integer endpoint,
                                  final Map<String, String> changedProperties) {
        this.transactionId = transactionId;
        this.time = time;
        this.endpoint = endpoint;
        this.changes = changedProperties;
    }

    private static boolean equals(final Object a, final Object b) {
        if (a == null) {
            return b == null;
        } else if (b == null) {
            return false;
        } else {
            return a.equals(b);
        }
    }

    @Override
    public Object process(final Map.Entry<String, ContainerState> entry) {

        ContainerState containerState = entry.getValue();

        if (changes == null) {
            return containerState;
        }

        if (containerState == null) {
            containerState = new ContainerState();
        }

        PositionState endpointState = containerState.getOrCreatePosition(endpoint);

        for (final Map.Entry<String, String> propertyEntry : changes.entrySet()) {

            final Property property = endpointState.getOrCreateProperty(propertyEntry.getKey());

            final String value = propertyEntry.getValue();
            if (!equals(property.actualValue, value)) {
                property.actualValue = value;
                property.actualChanged = time;
            }

            property.actualReceived = time;

            // Populate target fields even though we should not. This is to make results comparable with
            // other tests where we set actual and target together
            property.requestedValue = value;
            property.requestTime = time;
        }

        entry.setValue(containerState);

        return containerState;
    }
}

