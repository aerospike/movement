package com.aerospike.movement.emitter.files;

import com.aerospike.movement.runtime.core.driver.WorkChunk;
import com.aerospike.movement.runtime.core.driver.WorkChunkId;
import com.aerospike.movement.util.core.ErrorUtil;

import java.nio.file.Path;
import java.util.AbstractMap;
import java.util.Map;
import java.util.UUID;

public class WorkFile implements WorkChunk {
    private Path filePath;

    public WorkFile(final Path filePath) {

    }

    public static WorkChunk fromPath(final Path filePath) {
        return new WorkFile(filePath);
    }

    @Override
    public WorkChunkId next() {
        throw ErrorUtil.unimplemented();
    }

    @Override
    public boolean hasNext() {
        throw ErrorUtil.unimplemented();
    }

    @Override
    public UUID getId() {
        throw ErrorUtil.unimplemented();
    }

    public Path getPath() {
        return filePath;
    }

    public static class WorkFileEntryId extends WorkChunkId {
        private final Path filePath;
        public WorkFileEntryId(final Long id, final Path filePath) {
            super(new AbstractMap.SimpleEntry<>(filePath, id));
            this.filePath = filePath;
        }

        public Path getFilePath() {
            return filePath;
        }
        @Override
        public Map.Entry<Path,Long> getId() {
            return (Map.Entry<Path, Long>) id;
        }
    }
}
