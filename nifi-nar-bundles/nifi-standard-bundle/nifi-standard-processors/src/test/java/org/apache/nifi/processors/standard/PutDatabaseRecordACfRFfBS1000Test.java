package org.apache.nifi.processors.standard;

import org.apache.nifi.processor.util.pattern.RollbackOnFailure;
import org.junit.jupiter.api.BeforeEach;

public class PutDatabaseRecordACfRFfBS1000Test extends PutDatabaseRecordTest {
    @BeforeEach
    public void setRunner() throws Exception {
        super.setRunner();
        runner.setProperty(PutDatabaseRecord.AUTO_COMMIT, "false");
        runner.setProperty(RollbackOnFailure.ROLLBACK_ON_FAILURE, "false");
        runner.setProperty(PutDatabaseRecord.MAX_BATCH_SIZE, "1000");
    }
}
