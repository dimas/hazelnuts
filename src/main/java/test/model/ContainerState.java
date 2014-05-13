package test.model;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class ContainerState implements Serializable {

    public Map<Integer, PositionState> positions;

    public PositionState getOrCreatePosition(final Integer position) {
        if (positions == null) {
            positions = new HashMap<Integer, PositionState>();
        }

        PositionState result = positions.get(position);
        if (result == null) {
            result = new PositionState();
            positions.put(position, result);
        }

        return result;
    }
}
