package test.processors;

import com.hazelcast.map.AbstractEntryProcessor;
import test.model.ContainerState;
import test.model.PositionState;
import test.model.Property;

import java.util.Date;
import java.util.Map;

public class RequestedPropertiesSetter extends AbstractEntryProcessor<String, ContainerState> {

    private static final long serialVersionUID = 1971353509612936993L;

    private String requestId;
    private Date time;
    private Integer position;
    private final Map<String, String> changes;

    public RequestedPropertiesSetter(final Date time, final String requestId,
                                     final Integer position,
                                     final Map<String, String> changedProperties) {
        this.requestId = requestId;
        this.time = time;
        this.position = position;
        this.changes = changedProperties;
    }

    @Override
    public Object process(final Map.Entry<String, ContainerState> entry) {

        ContainerState containerState = entry.getValue();
        if (containerState == null) {
            containerState = new ContainerState();
        }

        PositionState endpointState = containerState.getOrCreatePosition(position);

        for (final Map.Entry<String, String> propertyEntry : changes.entrySet()) {

            final Property property = endpointState.getOrCreateProperty(propertyEntry.getKey());

            final String value = propertyEntry.getValue();
            property.requestedValue = value;
            property.requestId = requestId;
            property.requestTime = time;
        }

        entry.setValue(containerState);

        return containerState;
    }
}
