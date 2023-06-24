package com.aerospike.graph.move.runtime.distributed.role;

public abstract class Role {
    public static boolean assume(final Class<? extends Role> role) {
        return false;
    }

    public static boolean leave(final Class<? extends Role> role)  {
        return false;
    }

    public static boolean amICoordinator() {
        return false;
    }
}
