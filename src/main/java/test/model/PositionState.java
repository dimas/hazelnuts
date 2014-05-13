package test.model;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class PositionState implements Serializable {

    public Map<String, Property> properties;

    public Property getOrCreateProperty(final String name) {
        if (properties == null) {
            properties = new HashMap<String, Property>();
        }

        Property property = properties.get(name);
        if (property == null) {
            property = new Property();
            properties.put(name, property);
        }

        return property;
    }
}
