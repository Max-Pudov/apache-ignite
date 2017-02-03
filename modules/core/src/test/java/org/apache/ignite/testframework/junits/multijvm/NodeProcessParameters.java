package org.apache.ignite.testframework.junits.multijvm;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.jetbrains.annotations.Nullable;

/**
 * Immutable structure to hold node process parameters.
 */
public class NodeProcessParameters {
    /** The default node process parameters. */
    public static NodeProcessParameters DFLT = new NodeProcessParameters(false, false, null, true/*inheritEnv*/, null);

    /**
     * Creates parameters for external Ignite node.
     * Adds special JVM arg line identifier to simplify process tracking.
     *
     * @param nodeGridName The name of Ignite node in arbitrary form.
     * @return The node process parameters.
     */
    public static NodeProcessParameters forNode(String nodeGridName) {
        return new NodeProcessParameters(false, false, null, true, Arrays.asList("-ea", "-Dignite.node="+nodeGridName));
    }

    /** Unique work dir flag. */
    private final boolean uniqueWorkDir;

    /** Unique proc dir. */
    private final boolean uniqueProcDir;

    /** The environment. */
    private final Map<String, String> procEnv;

    /** The JVM arguments. */
    private final List<String> jvmArguments;

    /**
     * Constructor.
     *
     * @param uniqueWorkDir If the node working directory should be unique.
     * @param uniqueProcDir If the process should have unique current directory.
     * @param procEnv The process environment.
     * @param jvmArgs The JVM arguments.
     */
    public NodeProcessParameters(boolean uniqueWorkDir, boolean uniqueProcDir,
        @Nullable Map<String, String> procEnv, boolean inheritEnv, @Nullable List<String> jvmArgs) {
        this.uniqueWorkDir = uniqueWorkDir;
        this.uniqueProcDir = uniqueProcDir;

        if (inheritEnv) {
            A.ensure(procEnv == null, "If we inherit the environment env, don't pass the one in.");

            this.procEnv = System.getenv();
        } else
            this.procEnv = procEnv;

        this.jvmArguments = jvmArgs;
    }

    /**
     * @return The JVM arguments.
     */
    public List<String> getJvmArguments() {
        return jvmArguments;
    }

    /**
     * @return The process environment.
     */
    public Map<String, String> getProcEnv() {
        return procEnv;
    }

    /**
     * @return If the unique process directory should be used.
     */
    public boolean isUniqueProcDir() {
        return uniqueProcDir;
    }

    /**
     * @return If the unique node working dir should be used.
     */
    public boolean isUniqueWorkDir() {
        return uniqueWorkDir;
    }
}
