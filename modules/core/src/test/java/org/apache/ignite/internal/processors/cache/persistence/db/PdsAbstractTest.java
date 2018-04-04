package org.apache.ignite.internal.processors.cache.persistence.db;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;

public class PdsAbstractTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        if (!isDefaultDBWorkDirectoryEmpty())
            deleteDefaultDBWorkDirectory();

        assert isDefaultDBWorkDirectoryEmpty() : "DB work directory is not empty.";
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        //protection if test failed to finish, e.g. by error
        stopAllGrids();

        assert deleteDefaultDBWorkDirectory() : "Couldn't delete DB work directory.";
    }

    /**
     * Gets a path to the default DB working directory.
     *
     * @return Path to the default DB working directory.
     * @throws IgniteCheckedException In case of an error.
     * @see #deleteDefaultDBWorkDirectory()
     * @see #isDefaultDBWorkDirectoryEmpty()
     */
    protected Path getDefaultDbWorkPath() throws IgniteCheckedException {
        return Paths.get(U.defaultWorkDirectory() + File.separator + DFLT_STORE_DIR);
    }

    /**
     * Deletes the default DB working directory with all sub-directories and files.
     *
     * @return {@code true} if and only if the file or directory is successfully deleted, otherwise {@code false}.
     * @throws IgniteCheckedException In case of an error.
     * @see #getDefaultDbWorkPath()
     * @see #deleteDefaultDBWorkDirectory()
     */
    protected boolean deleteDefaultDBWorkDirectory() throws IgniteCheckedException {
        Path dir = getDefaultDbWorkPath();

        return Files.notExists(dir) || U.delete(dir.toFile());
    }

    /**
     * Checks if the default DB working directory is empty.
     *
     * @return {@code true} if the default DB working directory is empty or doesn't exist, otherwise {@code false}.
     * @throws IgniteCheckedException In case of an error.
     * @see #getDefaultDbWorkPath()
     * @see #deleteDefaultDBWorkDirectory()
     */
    @SuppressWarnings("ConstantConditions")
    protected boolean isDefaultDBWorkDirectoryEmpty() throws IgniteCheckedException {
        File dir = getDefaultDbWorkPath().toFile();

        return !dir.exists() || (dir.isDirectory() && dir.list().length == 0);
    }
}
